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

import csv

from datetime import datetime

import re

DOI_RE = re.compile(r"^10\.\d{4,9}/\S+$", re.IGNORECASE)

def _get_row_val(row, *candidates: str):
    """
    Acceso case-insensitive y con tolerancia a espacios en el header.
    """
    norm = {re.sub(r"\s+", " ", k.strip().lower()): v for k, v in row.items()}
    for cand in candidates:
        key = re.sub(r"\s+", " ", cand.strip().lower())
        if key in norm:
            val = norm[key]
            if val is not None and str(val).strip() != "":
                return str(val).strip()
    return None

def _has_springer_headers(fieldnames: list[str]) -> bool:
    if not fieldnames:
        return False
    norm = {re.sub(r"\s+", " ", (f or "").strip().lower()) for f in fieldnames}
    return ("item title" in norm or "title" in norm) and ("item doi" in norm or "doi" in norm)

def _has_ieee_headers(fieldnames: list[str]) -> bool:
    if not fieldnames:
        return False
    norm = {re.sub(r"\s+", " ", (f or "").strip().lower()) for f in fieldnames}
    # IEEE Xplore típicos
    return ("document title" in norm) and ("publication year" in norm or "year" in norm) and ("doi" in norm)

def _extract_row_springer(row):
    title = _get_row_val(row, "Item Title", "Title")
    year  = _get_row_val(row, "Publication Year", "Year")
    doi   = _get_row_val(row, "Item DOI", "DOI")
    return title, year, doi

def _extract_row_ieee(row):
    title = _get_row_val(row, "Document Title", "Title")
    year  = _get_row_val(row, "Publication Year", "Year")
    doi   = _get_row_val(row, "DOI")
    return title, year, doi

def parse_csv_dir(csv_dir: str, extensions: tuple[str, ...] = (".csv", ".tsv")):
    """
    CSV/TSV → conjuntos equivalentes a parse_bib_dir
    Soporta SpringerLink e IEEE Xplore (auto-detección por headers).
    Returns:
      - retrieved_ids: set canonical (doi:... o fallback_id)
      - retrieved_dois: set de DOIs
      - detail: lista dicts {id, doi, title, year, source_file}
    """
    retrieved_ids = set()
    retrieved_dois = set()
    detail = []

    for name in os.listdir(csv_dir):
        if not name.lower().endswith(extensions):
            continue
        path = os.path.join(csv_dir, name)
        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                sample = f.read(4096)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
                except Exception:
                    dialect = csv.excel_tab if name.lower().endswith(".tsv") else csv.excel

                reader = csv.DictReader(f, dialect=dialect)
                fns = reader.fieldnames or []

                if _has_springer_headers(fns):
                    extractor = _extract_row_springer
                elif _has_ieee_headers(fns):
                    extractor = _extract_row_ieee
                else:
                    print(f"[WARN] Skipping {name}: headers not recognized as Springer/IEEE")
                    continue

                for row in reader:
                    try:
                        title, year, doi_raw = extractor(row)
                        doi = clean_doi(doi_raw)
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
                    except Exception as ex:
                        print(f"[WARN] Skipped row in {name}: {ex}")
        except Exception as ex:
            print(f"[WARN] Failed to parse {name}: {ex}")

    return retrieved_ids, retrieved_dois, detail

def clean_doi(doi):
    if not doi:
        return None
    s = str(doi)
    s = unquote(s)
    # elimina espacios visibles y NO visibles dentro del DOI
    s = s.replace("\u200b","").replace("\u200c","").replace("\u200d","")
    s = re.sub(r"\s+", "", s)

    # prefijos de resolvers
    s = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", s, flags=re.I)

    # puntuación/residuos al final
    s = re.sub(r"[\]\).;,]+$", "", s).strip()

    # Valida patrón DOI
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


def compute_metrics(args, out_path=None, count_duplicates=False):
    # 1) Cargar resultados desde la fuente seleccionada
    if args.bibs_dir:
        retrieved_ids, retrieved_dois, detail = parse_bib_dir(args.bibs_dir)
        input_dir = args.bibs_dir
    else:
        retrieved_ids, retrieved_dois, detail = parse_csv_dir(args.csv_dir)
        input_dir = args.csv_dir

    # 2) Cargar relevantes (lista objetivo)
    relevant_dois = load_relevant(args)

    # 3) Métricas base
    ID = len(detail)
    TE = len(retrieved_ids)
    TE_doi = len(retrieved_dois)
    ER = len(relevant_dois)
    TER_dois = relevant_dois.intersection(retrieved_dois)
    TER = len(TER_dois)

    FN = relevant_dois.difference(retrieved_dois)
    FP = retrieved_dois.difference(relevant_dois)

    RC = 0.0 if ER == 0 else (TER / ER) * 100.0
    EF = 0.0 if TE == 0 else (TER / TE) * 100.0

    dups = _summarize_duplicates(detail)
    report = {
        "CB": getattr(args, "cb", None),
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
        "total_relevant": ER,
        "relevant_retrieved": TER,
        "studies_retrieved": TE,
        "sensitivity_percent": round(RC, 2),
        "precision_percent": round(EF, 2),
        "retrieved_counts": {
            "unique_ids_total": TE,
            "unique_dois_total": TE_doi,
        },
        "duplicates": dups,
    }

    if out_path:
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

    return report, input_dir, detail

def _summarize_duplicates(detail):
    by_id = {}
    by_doi = {}
    by_fb  = {}
    for d in detail:
        cid = d.get("id")
        doi = d.get("doi")
        by_id[cid] = by_id.get(cid, 0) + 1
        if doi:
            by_doi[doi] = by_doi.get(doi, 0) + 1
        elif cid and cid.startswith("ttlY:"):
            by_fb[cid] = by_fb.get(cid, 0) + 1

    dups_id = {k:v for k,v in by_id.items() if v>1}
    dups_doi = {k:v for k,v in by_doi.items() if v>1}
    dups_fb  = {k:v for k,v in by_fb.items() if v>1}

    return {
        "duplicates_collapsed_total": sum(v-1 for v in dups_id.values()),
        "duplicates_by_doi": sorted(([k,v] for k,v in dups_doi.items()), key=lambda x: -x[1]),
        "duplicates_by_fallback": sorted(([k,v] for k,v in dups_fb.items()), key=lambda x: -x[1]),
    }


def build_parser():
    p = argparse.ArgumentParser(description="Compute Zhang (2011) Sensitivity & Precision for a search.")
    
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--bibs-dir", help="Directory containing .bib files (results).")
    src.add_argument("--csv-dir",  help="Directory containing SpringerLink CSV/TSV files (results).")


    tg = p.add_mutually_exclusive_group(required=True)
    tg.add_argument("--targets-file", help="Text file containing your relevant studies list (any format; DOIs will be extracted).")
    tg.add_argument("--targets-dois", help="Comma/space separated DOIs string (e.g., '10.1016/... 10.1145/...').")

    p.add_argument("--cb", help="Cadena de búsqueda (CB) para registrar en el reporte.", default=None)

    p.add_argument("--out", help="Optional JSON report path.")
    return p

def resolve_output_path(out_arg: str | None, bibs_dir: str) -> str:
    """
    Returns a unique JSON file path.
    - If out_arg is None: create <bibs_dir>/reports/report_<timestamp>.json
    - If out_arg is an existing directory: put report_<timestamp>.json inside it
    - If out_arg looks like a file path: append _<timestamp> before extension
      (and add numeric suffix if still collides)
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    def ensure_dir(d: str):
        os.makedirs(d, exist_ok=True)

    # Case A: no --out provided → default dir inside bibs_dir
    if out_arg is None:
        base_dir = os.path.join(os.path.abspath(bibs_dir), "reports")
        ensure_dir(base_dir)
        return os.path.join(base_dir, f"report_{ts}.json")

    # Case B: --out is a directory
    if os.path.isdir(out_arg):
        base_dir = os.path.abspath(out_arg)
        ensure_dir(base_dir)
        return os.path.join(base_dir, f"report_{ts}.json")

    # Case C: --out looks like a file path
    base_dir = os.path.dirname(out_arg) or "."
    base_name = os.path.basename(out_arg)
    root, ext = os.path.splitext(base_name)
    if not ext:
        ext = ".json"

    ensure_dir(base_dir)

    candidate = os.path.join(base_dir, f"{root}_{ts}{ext}")
    if not os.path.exists(candidate):
        return candidate

    i = 2
    while True:
        cand = os.path.join(base_dir, f"{root}_{ts}_{i}{ext}")
        if not os.path.exists(cand):
            return cand
        i += 1

def main():
    parser = build_parser()
    args = parser.parse_args()
    setattr(args, "cb", args.cb)
    
    if args.targets_file and not os.path.isfile(args.targets_file):
        print(f"[ERROR] targets file not found: {args.targets_file}", file=sys.stderr)
        sys.exit(2)
    
    base_input_dir = args.bibs_dir or args.csv_dir
    out_path = resolve_output_path(args.out, base_input_dir)

    report, input_dir_used, detail = compute_metrics(args, out_path=out_path)


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
        print(f"\nSaved JSON report -> {out_path}")
    
    if report.get("duplicates"):
        d = report["duplicates"]
        print(f"\nDuplicados colapsados: {d['duplicates_collapsed_total']}")
        if d["duplicates_by_doi"]:
            print("  (por DOI, top 5):")
            for doi, cnt in d["duplicates_by_doi"][:5]:
                print(f"    - {doi} x{cnt}")
        if d["duplicates_by_fallback"]:
            print("  (por fallback title+year, top 5):")
            for fb, cnt in d["duplicates_by_fallback"][:5]:
                print(f"    - {fb} x{cnt}")


"""
---
ACM
---

PYTHONPATH=src \
poetry run python -m src.zhang_metrics \
  --bibs-dir /home/ivan/Downloads/cadenas/input/acm \
  --targets-file /home/ivan/Downloads/cadenas/input/acm/target.txt \
  --cb '

  ' \
  --out /home/ivan/Downloads/cadenas/output/acm/

  



---
ScienceDirect
---

PYTHONPATH=src \
poetry run python -m src.zhang_metrics \
  --bibs-dir /home/ivan/Downloads/cadenas/input/sciencedirect \
  --targets-file /home/ivan/Downloads/cadenas/input/sciencedirect/target.txt \
  --cb '' \
  --out /home/ivan/Downloads/cadenas/output/sciencedirect/

  

---
SpringerLink
---


PYTHONPATH=src \
poetry run python -m src.zhang_metrics \
  --csv-dir /home/ivan/Downloads/cadenas/input/springerlink/ \
  --targets-file /home/ivan/Downloads/cadenas/input/springerlink/target.txt \
  --cb '' \
  --out /home/ivan/Downloads/cadenas/output/springerlink/

---
IEEE
---

poetry run python -m src.zhang_metrics \
  --csv-dir /home/ivan/Downloads/cadenas/input/ieee/ \
  --targets-file /home/ivan/Downloads/cadenas/input/ieee/target.txt \
  --cb '
  
  ' \
  --out /home/ivan/Downloads/cadenas/output/ieee/


"""

if __name__ == "__main__":
    main()
