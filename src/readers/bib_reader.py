"""
BibTeX Normalization Pipeline (extensible, provider-aware)

Usage (example):

    from bib_normalizer import normalize_bib_dir, dump_ndjson

    entries = normalize_bib_dir("/path/to/bibs")
    dump_ndjson(entries, "/path/to/out/entries.ndjson")

Design:
- Base normalization (provider-agnostic) produces a uniform schema.
- Provider adapters (ScienceDirect, ACM, ...) detect entries and apply fixups.
- Easy to extend: implement BaseProvider.detect() and .fix(). Add to PROVIDERS.

Code style: English only, minimal type hints (only where obvious).
"""

import os
import re
import json
import hashlib
from typing import List, Dict, Any, Optional, Tuple

import bibtexparser
from bibtexparser.bparser import BibTexParser
from bibtexparser.customization import convert_to_unicode

# -----------------------------
# Utilities
# -----------------------------

def _slugify(text: str) -> str:
    text = text or ""
    text = text.lower()
    text = re.sub(r"https?://doi\.org/", "", text.strip())
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-{2,}", "-", text).strip("-")


def _clean_doi(doi: Optional[str]) -> Optional[str]:
    if not doi:
        return None
    doi = doi.strip()
    doi = re.sub(r"^https?://doi\.org/", "", doi, flags=re.I)
    return doi or None


def _split_authors(author_field: Optional[str]) -> List[Dict[str, str]]:
    if not author_field:
        return []
    parts = [a.strip() for a in author_field.replace("\n", " ").split(" and ") if a.strip()]
    authors = []
    for p in parts:
        # Prefer BibTeX "Last, First"; else fall back to last token as last name
        if "," in p:
            last, first = [x.strip() for x in p.split(",", 1)]
        else:
            tokens = p.split()
            last = tokens[-1].strip() if tokens else ""
            first = " ".join(tokens[:-1]).strip()
        authors.append({"full": p.strip(), "last": last, "first": first})
    return authors


def _split_keywords(kw: Optional[str]) -> List[str]:
    if not kw:
        return []
    raw = re.split(r"[;,]\s*|\n+", kw)
    cleaned = [k.strip() for k in raw if k and k.strip()]
    seen = set()
    result = []
    for k in cleaned:
        lk = k.lower()
        if lk not in seen:
            seen.add(lk)
            result.append(k)
    return result


def _prefer_venue(entry: Dict[str, Any]) -> Optional[str]:
    for key in ("journal", "booktitle", "howpublished"):
        v = entry.get(key)
        if v:
            return v
    return None


def _safe_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    try:
        return int(str(s).strip())
    except Exception:
        return None


def _compute_hash(title: Optional[str], authors: List[Dict[str, str]], year: Optional[int]) -> str:
    a = ",".join([a["full"] for a in authors]) if authors else ""
    y = str(year) if year is not None else ""
    base = f"{title or ''}||{a}||{y}"
    return "sha1:" + hashlib.sha1(base.encode("utf-8")).hexdigest()


def _parse_pages(pages: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    if not pages:
        return None, None, None
    txt = str(pages).strip()
    # Normalize separators (en dash, em dash, hyphen)
    txt_norm = re.sub(r"[\u2012\u2013\u2014\-]", "-", txt)
    m = re.match(r"\s*(\d+)\s*-(\d+)\s*$", txt_norm)
    if m:
        start, end = m.group(1), m.group(2)
        try:
            n = int(end) - int(start) + 1
        except Exception:
            n = None
        return start, end, n
    # If single number or non-standard, keep as is
    return None, None, None


def _detect_venue_type(entry_type: Optional[str], venue: Optional[str], series: Optional[str]) -> Optional[str]:
    et = (entry_type or "").lower()
    if et == "article":
        return "journal"
    if et in ("inproceedings", "proceedings"):  # ACM/IEEE conferences
        return "conference"
    if et in ("book", "inbook"):
        return "book"
    if et in ("phdthesis", "mastersthesis", "bachelorthesis"):
        return "thesis"
    # Fallbacks based on series hints
    if series:
        s = series.lower()
        if "icse" in s or "gecco" in s or "issta" in s or "ase" in s:
            return "conference"
    return None


# -----------------------------
# Provider adapters (extensible)
# -----------------------------

class BaseProvider:
    name = "base"

    @staticmethod
    def detect(raw_entry: Dict[str, Any]) -> bool:
        return False

    @staticmethod
    def fix(normalized: Dict[str, Any], raw_entry: Dict[str, Any]) -> Dict[str, Any]:
        return normalized


class ScienceDirectProvider(BaseProvider):
    name = "sciencedirect"

    @staticmethod
    def detect(raw_entry: Dict[str, Any]) -> bool:
        url = str(raw_entry.get("url", "")).lower()
        doi = str(raw_entry.get("doi", "")).lower()
        return "sciencedirect.com" in url or doi.startswith("10.1016/")

    @staticmethod
    def fix(normalized: Dict[str, Any], raw_entry: Dict[str, Any]) -> Dict[str, Any]:
        normalized["source_provider"] = "sciencedirect"
        normalized.setdefault("source_platform", "ScienceDirect")
        # Keep publisher if given, else avoid guessing (Elsevier is common but not universal)
        doi = normalized.get("doi")
        if doi and not normalized.get("doi_resolver_url"):
            normalized["doi_resolver_url"] = f"https://doi.org/{doi}"
        # Mark venue type if missing
        if not normalized.get("venue_type"):
            normalized["venue_type"] = _detect_venue_type(normalized.get("entry_type"), normalized.get("venue"), normalized.get("series"))
        return normalized


class ACMProvider(BaseProvider):
    name = "acm"

    @staticmethod
    def detect(raw_entry: Dict[str, Any]) -> bool:
        url = str(raw_entry.get("url", "")).lower()
        doi = str(raw_entry.get("doi", "")).lower()
        publisher = str(raw_entry.get("publisher", "")).lower()
        return (
            "dl.acm.org" in url
            or "acm.org" in url
            or doi.startswith("10.1145/")
            or "association for computing machinery" in publisher
        )

    @staticmethod
    def fix(normalized: Dict[str, Any], raw_entry: Dict[str, Any]) -> Dict[str, Any]:
        normalized["source_provider"] = "acm"
        normalized.setdefault("source_platform", "ACM Digital Library")
        # Normalize publisher name if ACM
        pub = normalized.get("publisher") or raw_entry.get("publisher")
        if pub and "association for computing machinery" in str(pub).lower():
            normalized["publisher"] = "Association for Computing Machinery"
        # DOI resolver url
        doi = normalized.get("doi")
        if doi and not normalized.get("doi_resolver_url"):
            normalized["doi_resolver_url"] = f"https://doi.org/{doi}"
        # Venue type tends to be conference for inproceedings
        if not normalized.get("venue_type"):
            normalized["venue_type"] = _detect_venue_type(normalized.get("entry_type"), normalized.get("venue"), normalized.get("series"))
        return normalized


# Registry of providers (order matters; first match wins)
PROVIDERS: List[BaseProvider] = [
    ScienceDirectProvider,
    ACMProvider,
]


# -----------------------------
# Normalization core
# -----------------------------

def _normalize_raw_entry(raw: Dict[str, Any], src_file: str, src_folder: str) -> Dict[str, Any]:
    # bibtexparser uses upper-case keys ID/ENTRYTYPE and lower-case field names.
    # Create case-insensitive access
    e = {}
    for k, v in raw.items():
        e[k] = v
        e[k.lower()] = v

    entry_type = e.get("ENTRYTYPE") or e.get("entrytype")
    bib_id = e.get("ID") or e.get("id") or e.get("key")

    title = e.get("title")
    authors = _split_authors(e.get("author"))
    year = _safe_int(e.get("year"))

    venue = _prefer_venue(e)
    volume = e.get("volume")
    number = e.get("number")
    pages_text = e.get("pages")
    page_start, page_end, np_from_range = _parse_pages(pages_text)

    publisher = e.get("publisher")
    issn = e.get("issn")
    isbn = e.get("isbn")

    doi = _clean_doi(e.get("doi"))
    url = e.get("url")
    keywords = _split_keywords(e.get("keywords"))
    abstract = e.get("abstract")
    series = e.get("series")
    address = e.get("address")  # ACM often has city/state here
    location = e.get("location") # ACM explicit location
    numpages_field = _safe_int(e.get("numpages"))
    # prefer explicit numpages over computed
    numpages = numpages_field if numpages_field is not None else np_from_range

    key_normalized = _slugify(f"{authors[0]['last'] if authors else ''}-{year or ''}-{title or ''}") if title else None
    doc_hash = _compute_hash(title, authors, year)

    normalized = {
        "id": bib_id,
        "entry_type": (entry_type or "").lower() if entry_type else None,
        "title": title,
        "authors": authors,
        "year": year,
        "venue": venue,
        "venue_type": _detect_venue_type(entry_type, venue, series),
        "volume": volume,
        "number": number,
        "pages_text": pages_text,
        "page_start": page_start,
        "page_end": page_end,
        "numpages": numpages,
        "publisher": publisher,
        "issn": issn,
        "isbn": isbn,
        "doi": doi,
        "doi_resolver_url": f"https://doi.org/{doi}" if doi else None,
        "url": url,
        "keywords": keywords,
        "abstract": abstract,
        "series": series,
        "address": address,
        "location": location,
        "source_file": os.path.basename(src_file),
        "source_folder": src_folder,
        "source_platform": None,   # will be set by provider
        "source_provider": None,   # will be set by provider
        "cite_key": bib_id,
        "key_normalized": key_normalized,
        "hash": doc_hash,
        "tags": [],
        "extra": {"raw": raw},
    }
    # Mark if URL seems proxied
    if url and "ezproxy" in str(url).lower():
        normalized["url_via_proxy"] = True
    else:
        normalized["url_via_proxy"] = False

    return normalized


def _apply_providers(normalized: Dict[str, Any], raw: Dict[str, Any]) -> Dict[str, Any]:
    for provider_cls in PROVIDERS:
        try:
            if provider_cls.detect(raw):
                normalized = provider_cls.fix(normalized, raw)
                break
        except Exception:
            # Do not fail the whole pipeline on provider errors; continue gracefully
            continue
    return normalized


def read_bib_file(file_bib_path: str) -> List[Dict[str, Any]]:
    with open(file_bib_path, encoding="utf-8") as bibfile:
        parser = BibTexParser(common_strings=True)
        parser.customization = convert_to_unicode
        bib_database = bibtexparser.load(bibfile, parser=parser)

    results: List[Dict[str, Any]] = []
    for raw_entry in bib_database.entries:
        normalized = _normalize_raw_entry(raw_entry, src_file=file_bib_path, src_folder=os.path.dirname(file_bib_path))
        normalized = _apply_providers(normalized, raw_entry)
        results.append(normalized)
    return results


def normalize_bib_dir(dirpath: str, file_extension: str = ".bib") -> List[Dict[str, Any]]:
    """
    Walk a directory, parse every *.bib, and return a list of normalized entries.
    """
    all_entries: List[Dict[str, Any]] = []
    for filename in os.listdir(dirpath):
        if filename.lower().endswith(file_extension.lower()):
            file_path = os.path.join(dirpath, filename)
            try:
                entries = read_bib_file(file_path)
                all_entries.extend(entries)
            except Exception as ex:
                print(f"[WARN] Failed to parse {file_path}: {ex}")

    for e in all_entries:
        yr = e.get("year")
        et = e.get("entry_type")
        title = (e.get("title") or "").replace("\n", " ")
        title_short = (title[:77] + "...") if len(title) > 80 else title
        src = e.get("source_provider") or "?"
        print(f"• {e['id']} | {et} | {yr} | {src} | {title_short}")

    return all_entries


def dump_ndjson(entries: List[Dict[str, Any]], out_path: str) -> None:
    """
    Save entries to NDJSON (one JSON object per line).
    """
    with open(out_path, "w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


# -----------------------------
# Extending the pipeline
# -----------------------------
# To add a new provider (e.g., IEEE Xplore), create a class like:
#
# class IEEEProvider(BaseProvider):
#     name = "ieee"
#     @staticmethod
#     def detect(raw_entry: Dict[str, Any]) -> bool:
#         url = str(raw_entry.get("url", "")).lower()
#         doi = str(raw_entry.get("doi", "")).lower()
#         publisher = str(raw_entry.get("publisher", "")).lower()
#         return "ieeexplore.ieee.org" in url or doi.startswith("10.1109/") or "ieee" in publisher
#
#     @staticmethod
#     def fix(normalized: Dict[str, Any], raw_entry: Dict[str, Any]) -> Dict[str, Any]:
#         normalized["source_provider"] = "ieee"
#         normalized.setdefault("source_platform", "IEEE Xplore")
#         # Set venue type if missing
#         if not normalized.get("venue_type"):
#             normalized["venue_type"] = _detect_venue_type(normalized.get("entry_type"), normalized.get("venue"), normalized.get("series"))
#         return normalized
#
# Then add IEEEProvider to PROVIDERS (order matters).


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Normalize BibTeX files into a unified JSON schema.")
    parser.add_argument("dir", help="Directory containing .bib files")
    parser.add_argument("--out", dest="out", default=None, help="NDJSON output path (optional)")
    args = parser.parse_args()

    entries_ = normalize_bib_dir(args.dir)
    if args.out:
        dump_ndjson(entries_, args.out)
        print(f"Saved {len(entries_)} entries to {args.out}")
    else:
        print(f"Parsed {len(entries_)} entries (no output file requested)")
