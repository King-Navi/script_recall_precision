import argparse
import os
import sys
import json
import re
import hashlib
from urllib.parse import unquote

import bibtexparser
from bibtexparser.bparser import BibTexParser
from bibtexparser.customization import convert_to_unicode

import re

DOI_RE = re.compile(r"^10\.\d{4,9}/\S+$", re.IGNORECASE)


def clean_doi(doi):
    if not doi:
        return None
    s = str(doi).strip().replace("\n", " ")

    # Keep only real DOI resolver prefixes; do NOT strip arbitrary hosts
    s = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", s, flags=re.I)

    # Trim trailing punctuation, spaces, or closing brackets
    s = re.sub(r"[\s\]\).;,]+$", "", s).strip()

    # Validate final token truly looks like a DOI
    if not DOI_RE.match(s):
        return None
    return s.lower()


def fallback_id(title, year):
    t = (title or "").strip().lower()
    y = (str(year).strip() if year else "")
    base = f"{t}||{y}"
    return "ttlY:" + hashlib.sha1(base.encode("utf-8")).hexdigest()


def parse_bib_dir(bibs_dir):
    """
    Returns:
      - retrieved_ids: set of canonical IDs (prefer DOI; fallback to title-year hash)
      - retrieved_dois: set of DOIs only (subset of retrieved_ids)
      - detail: list of dicts with {id, doi, title, year, source_file}
    """
    parser = BibTexParser(common_strings=True)
    parser.customization = convert_to_unicode

    retrieved_ids = set()
    retrieved_dois = set()
    detail = []

    for name in os.listdir(bibs_dir):
        if not name.lower().endswith(".bib"):
            continue
        path = os.path.join(bibs_dir, name)
        with open(path, encoding="utf-8") as f:
            db = bibtexparser.load(f, parser=parser)

        for e in db.entries:
            doi = clean_doi(e.get("doi") or e.get("DOI"))
            title = e.get("title")
            year = e.get("year")
            if doi:
                cid = f"doi:{doi}"
                retrieved_dois.add(doi)
            else:
                cid = fallback_id(title, year)

            retrieved_ids.add(cid)
            detail.append({
                "id": cid,
                "doi": doi,
                "title": title,
                "year": year,
                "source_file": name,
            })

    return retrieved_ids, retrieved_dois, detail


def extract_dois_from_text(text):
    """
    Extract DOIs from arbitrary text.
    Accepts raw DOIs (10.xxxx/...) and DOI resolver URLs (https://doi.org/10.xxxx/...).
    Ignores non-DOI URLs (e.g., ScienceDirect article pages).
    """
    dois = set()

    # Bare DOIs
    for tok in re.findall(r"10\.\d{4,9}/\S+", text, flags=re.I):
        d = clean_doi(tok)
        if d:
            dois.add(d)

    # DOI resolver URLs
    for tok in re.findall(r"https?://(?:dx\.)?doi\.org/\S+", text, flags=re.I):
        d = clean_doi(tok)
        if d:
            dois.add(d)

    return dois

def load_relevant(args):
    """
    Load relevant DOIs from either --targets-file (free text) or --targets-dois (comma/space separated).
    Returns set of DOIs (unique).
    """
    if args.targets_file:
        with open(args.targets_file, encoding="utf-8") as f:
            text = f.read()
        return extract_dois_from_text(text)

    if args.targets_dois:
        # Accept comma/space separated list
        raw = re.split(r"[\s,]+", args.targets_dois)
        dois = set()
        for r in raw:
            d = clean_doi(r)
            if d:
                dois.add(d)
        return dois

    print("[ERROR] Provide --targets-file or --targets-dois", file=sys.stderr)
    sys.exit(2)


def compute_metrics(bibs_dir, relevant_source, out_path=None, count_duplicates=False):
    retrieved_ids, retrieved_dois, detail = parse_bib_dir(bibs_dir)
    relevant_dois = load_relevant(relevant_source)

    ID = len(detail)
    TE = len(retrieved_ids)     # Total de estudios obtenidos
    TE_doi = len(retrieved_dois)# Unicos con DOI
    ER = len(relevant_dois)     # Total de estudios relevantes (tu lista objetivo)
    TER_dois = relevant_dois.intersection(retrieved_dois)  # relevantes recuperados (por DOI)
    TER = len(TER_dois)

    # Falsos/faltantes por DOI (para auditoría)

    #(False Negatives / Faltantes) = Relevantes que NO se recuperaron.
    FN = relevant_dois.difference(retrieved_dois)
    
    #FP (False Positives / No relevantes) = Recuperados que NO están en tu lista relevante.
    FP = retrieved_dois.difference(relevant_dois)
    
    # Métricas (Zhang 2011):
    # RC = TER / ER
    RC = 0.0 if ER == 0 else (TER / ER) * 100.0
    # EF = TER / TE
    EF = 0.0 if TE == 0 else (TER / TE) * 100.0

    report = {
        "CB": getattr(relevant_source, "cb", None),
        "ID": ID,
        "TE": TE,
        "TE_doi": TE_doi,
        "ER": ER,
        "TER": TER,
        "EF_percent": round(EF, 2),
        "RC_percent": round(RC, 2),

        "TP_dois": sorted(TER_dois),
        "FN_dois": sorted(FN),
        "FP_dois": sorted(FP),

        # Claves significado
        "total_relevant": ER,
        "relevant_retrieved": TER,
        "studies_retrieved": TE,
        "sensitivity_percent": round(RC, 2),
        "precision_percent": round(EF, 2),
        "retrieved_counts": {
            "unique_ids_total": TE,
            "unique_dois_total": TE_doi,
        },
    }

    if out_path:
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

    return report

def build_parser():
    p = argparse.ArgumentParser(description="Compute Zhang (2011) Sensitivity & Precision for a search.")
    p.add_argument("--bibs-dir", required=True, help="Directory containing .bib files (results).")

    tg = p.add_mutually_exclusive_group(required=True)
    tg.add_argument("--targets-file", help="Text file containing your relevant studies list (any format; DOIs will be extracted).")
    tg.add_argument("--targets-dois", help="Comma/space separated DOIs string (e.g., '10.1016/... 10.1145/...').")

    p.add_argument("--cb", help="Cadena de búsqueda (CB) para registrar en el reporte.", default=None)

    p.add_argument("--out", help="Optional JSON report path.")
    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    setattr(args, "cb", args.cb)

    report = compute_metrics(args.bibs_dir, args, out_path=args.out)

    print("\n=== Zhang (2011) Metrics ===")
    if report.get("CB"):
        print(f"CB: {report['CB']}")
    print(f"ID (Identificados, crudos): {report['ID']}")
    print(f"TE (Únicos): {report['TE']}  | TE_doi: {report['TE_doi']}")
    print(f"ER (Relevantes totales): {report['ER']}")
    print(f"TER (Relevantes recuperados): {report['TER']}")
    print(f"RC (Recall/Sensibilidad) = TER/ER * 100 = {report['RC_percent']}%")
    print(f"EF (Effort/Precisión)    = TER/TE * 100 = {report['EF_percent']}%")

    if report["FN_dois"]:
        print("\nMissing relevant (FN):")
        for d in report["FN_dois"]:
            print("  -", d)

    if report["FP_dois"]:
        print("\nFalse positives vs relevant list (FP, by DOI):")
        for d in report["FP_dois"]:
            print("  -", d)

    if report["TP_dois"]:
        print("\nRelevantes recuperados (TP, DOIs):")
        for d in report["TP_dois"]:
            print("  -", d)
    if args.out:
        print(f"\nSaved JSON report -> {args.out}")


"""
---
ScienceDirect
---

PYTHONPATH=src \
poetry run python -m src.zhang_metrics \
  --bibs-dir /home/ivan/Downloads/cadenas/input/sciencedirect \
  --targets-file /home/ivan/Downloads/cadenas/input/sciencedirect/target.txt \
  --out /home/ivan/Downloads/cadenas/output/sciencedirect/report.json


"""

if __name__ == "__main__":
    main()
