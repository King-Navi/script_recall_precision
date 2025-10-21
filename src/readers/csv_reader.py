# csv_reader.py
# SpringerLink CSV/TSV normalizer → unified schema compatible with bib_reader
# Fields expected (case/spacing tolerant):
#   "Item Title", "Publication Title", "Book Series Title", "Journal Volume",
#   "Journal Issue", "Item DOI", "Authors", "Publication Year", "URL", "Content Type"

from __future__ import annotations
import csv
import os
import re
import json
import hashlib
from typing import List, Dict, Any, Optional

DOI_RE = re.compile(r"^10\.\d{4,9}/\S+$", re.IGNORECASE)

def _safe_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    try:
        return int(str(s).strip())
    except Exception:
        return None

def _slugify(text: str) -> str:
    text = (text or "").lower().strip()
    text = re.sub(r"https?://doi\.org/", "", text)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-{2,}", "-", text).strip("-")

def _compute_hash(title: Optional[str], authors: List[Dict[str, str]], year: Optional[int]) -> str:
    a = ",".join([a["full"] for a in authors]) if authors else ""
    y = str(year) if year is not None else ""
    base = f"{title or ''}||{a}||{y}"
    return "sha1:" + hashlib.sha1(base.encode("utf-8")).hexdigest()

def _clean_doi(doi: Optional[str]) -> Optional[str]:
    if not doi:
        return None
    s = str(doi).strip().replace("\n", " ")
    s = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", s, flags=re.I)
    s = re.sub(r"[\s\]\).;,]+$", "", s).strip()
    if not DOI_RE.match(s):
        return None
    return s.lower()

def _split_authors_csv(author_field: Optional[str]) -> List[Dict[str, str]]:
    """
    Springer CSV suele separar autores por ';'.
    También tolera ' and ' o '|' por si acaso.
    Cada autor normalmente viene como 'Apellidos, Nombre'.
    """
    if not author_field:
        return []
    raw = re.split(r"\s*;\s*|\s+\band\b\s+|\s*\|\s*", author_field.strip())
    out = []
    for p in [x for x in raw if x]:
        if "," in p:
            last, first = [x.strip() for x in p.split(",", 1)]
        else:
            tokens = p.split()
            last = tokens[-1].strip() if tokens else ""
            first = " ".join(tokens[:-1]).strip()
        out.append({"full": p.strip(), "last": last, "first": first})
    return out

def _detect_venue_type(content_type: Optional[str]) -> Optional[str]:
    ct = (content_type or "").lower()
    if "journal" in ct or "article" in ct:
        return "journal"
    if "conference" in ct or "proceeding" in ct:
        return "conference"
    if "chapter" in ct or "book" in ct:
        return "book"
    return None

def _get_row_val(row: Dict[str, Any], *candidates: str) -> Optional[str]:
    # case/space-insensitive access
    lowmap = {re.sub(r"\s+", " ", k.strip().lower()): v for k, v in row.items()}
    for cand in candidates:
        key = re.sub(r"\s+", " ", cand.strip().lower())
        if key in lowmap and str(lowmap[key]).strip() != "":
            return str(lowmap[key]).strip()
    return None


def _normalize_row(row: Dict[str, Any], source_file: str, source_folder: str) -> Dict[str, Any]:
    title = _get_row_val(row, "Item Title", "Title")
    venue = _get_row_val(row, "Publication Title")
    series = _get_row_val(row, "Book Series Title", "Series Title")
    volume = _get_row_val(row, "Journal Volume", "Volume")
    number = _get_row_val(row, "Journal Issue", "Issue")
    doi_raw = _get_row_val(row, "Item DOI", "DOI")
    doi = _clean_doi(doi_raw)
    url = _get_row_val(row, "URL", "Landing Page")
    authors = _split_authors_csv(_get_row_val(row, "Authors", "Author"))
    year = _safe_int(_get_row_val(row, "Publication Year", "Year"))
    content_type = _get_row_val(row, "Content Type")

    entry_type = None
    vt = _detect_venue_type(content_type)
    # Map to bibtex-like entry_type to mirror bib_reader
    if vt == "journal":
        entry_type = "article"
    elif vt == "conference":
        entry_type = "inproceedings"
    elif vt == "book":
        entry_type = "inbook" if content_type and "chapter" in content_type.lower() else "book"

    # choose a cite_key/id: prefer DOI, else slug on first author + year + title
    cite_key = doi if doi else _slugify(f"{authors[0]['last'] if authors else ''}-{year or ''}-{title or ''}")

    normalized = {
        "id": cite_key,
        "entry_type": entry_type,
        "title": title,
        "authors": authors,
        "year": year,
        "venue": venue,
        "venue_type": vt,
        "volume": volume,
        "number": number,
        "pages_text": None,
        "page_start": None,
        "page_end": None,
        "numpages": None,
        "publisher": None,   # not present in this export
        "issn": None,
        "isbn": None,
        "doi": doi,
        "doi_resolver_url": f"https://doi.org/{doi}" if doi else None,
        "url": url,
        "keywords": [],
        "abstract": None,
        "series": series,
        "address": None,
        "location": None,
        "source_file": os.path.basename(source_file),
        "source_folder": source_folder,
        "source_platform": "SpringerLink",
        "source_provider": "springerlink",
        "cite_key": cite_key,
        "key_normalized": _slugify(f"{authors[0]['last'] if authors else ''}-{year or ''}-{title or ''}") if title else None,
        "hash": _compute_hash(title, authors, year),
        "tags": [],
        "extra": {"raw": row},
        "url_via_proxy": bool(url and "ezproxy" in url.lower()),
        "content_type": content_type,
    }
    return normalized

def read_csv_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Parse one SpringerLink CSV/TSV file and return normalized entries.
    Auto-detects delimiter between ',', '\\t', ';'
    """
    entries: List[Dict[str, Any]] = []
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
        except Exception:
            # fallback: comma
            dialect = csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        for row in reader:
            try:
                norm = _normalize_row(row, source_file=file_path, source_folder=os.path.dirname(file_path))
                entries.append(norm)
            except Exception as ex:
                print(f"[WARN] Skipped row in {os.path.basename(file_path)}: {ex}")
    return entries

def normalize_csv_dir(dirpath: str, file_extension: str = ".csv") -> List[Dict[str, Any]]:
    """
    Walk a directory, parse every *.csv (or .tsv if you pass file_extension), return normalized entries.
    """
    all_entries: List[Dict[str, Any]] = []
    for filename in os.listdir(dirpath):
        if filename.lower().endswith(file_extension.lower()):
            path = os.path.join(dirpath, filename)
            try:
                all_entries.extend(read_csv_file(path))
            except Exception as ex:
                print(f"[WARN] Failed to parse {path}: {ex}")

    # Compact summary (mirrors bib_reader)
    for e in all_entries:
        yr = e.get("year")
        et = e.get("entry_type")
        title = (e.get("title") or "").replace("\n", " ")
        title_short = (title[:77] + "...") if len(title) > 80 else title
        print(f"• {e['id']} | {et} | {yr} | springerlink | {title_short}")

    return all_entries

def dump_ndjson(entries: List[Dict[str, Any]], out_path: str) -> None:
    with open(out_path, "w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")



# CLI (optional)
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Normalize SpringerLink CSV/TSV into unified JSON schema.")
    ap.add_argument("dir", help="Directory containing .csv/.tsv files")
    ap.add_argument("--ext", default=".csv", help="File extension to scan (default: .csv). Use .tsv if needed.")
    ap.add_argument("--out", help="NDJSON output path (optional)")
    args = ap.parse_args()

    items = normalize_csv_dir(args.dir, file_extension=args.ext)
    if args.out:
        dump_ndjson(items, args.out)
        print(f"Saved {len(items)} entries to {args.out}")
    else:
        print(f"Parsed {len(items)} entries (no output file requested)")
