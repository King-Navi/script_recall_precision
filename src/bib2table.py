from __future__ import annotations
import os, re, argparse
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd

DOMAIN_DB_MAP = {
    'sciencedirect.com': 'ScienceDirect',
    'elsevier.com': 'ScienceDirect',
    'dl.acm.org': 'ACM DL',
    'acm.org': 'ACM DL',
    'ieeexplore.ieee.org': 'IEEE Xplore',
    'ieee.org': 'IEEE Xplore',
    'link.springer.com': 'SpringerLink',
    'springer.com': 'SpringerLink',
    'doi.org': 'DOI',
}

def split_bibtex_entries(text: str) -> List[str]:
    entries, i, n = [], 0, len(text)
    while i < n:
        at = text.find('@', i)
        if at == -1: break
        br_open = text.find('{', at)
        if br_open == -1: break
        depth, j = 1, br_open + 1
        while j < n and depth > 0:
            ch = text[j]
            if ch == '{': depth += 1
            elif ch == '}': depth -= 1
            j += 1
        if depth == 0:
            entries.append(text[at:j]); i = j
        else:
            entries.append(text[at:]); break
    return entries

def _parse_bib_value(s: str, pos: int):
    L = len(s)
    if pos >= L: return ("", pos)
    ch = s[pos]
    if ch == '{':
        depth, pos, buf = 1, pos+1, []
        while pos < L and depth > 0:
            c = s[pos]
            if c == '{': depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    pos += 1; break
            buf.append(c); pos += 1
        return (''.join(buf), pos)
    elif ch == '"':
        pos, buf = pos+1, []
        while pos < L:
            c = s[pos]
            if c == '"' and (pos == 0 or s[pos-1] != '\\'):
                pos += 1; break
            buf.append(c); pos += 1
        return (''.join(buf), pos)
    else:
        buf = []
        while pos < L and s[pos] not in ',}':
            buf.append(s[pos]); pos += 1
        return (''.join(buf), pos)

def _clean_bib_value(v: str) -> str:
    import re
    v2 = v.strip()
    while v2.startswith('{') and v2.endswith('}'):
        depth, balanced = 0, True
        for i, ch in enumerate(v2):
            if ch == '{': depth += 1
            elif ch == '}':
                depth -= 1
                if depth < 0: balanced = False; break
            if depth == 0 and i != len(v2) - 1:
                balanced = False; break
        if balanced: v2 = v2[1:-1].strip()
        else: break
    v2 = re.sub(r'\s+\n', ' ', v2)
    v2 = re.sub(r'\s+', ' ', v2).strip()
    return v2

def parse_entry(entry_text: str):
    import re
    m = re.match(r'\s*@\s*([A-Za-z]+)\s*{', entry_text, flags=re.S)
    if not m: return ("unknown", "unknown_key", {})
    entry_type = m.group(1).strip().lower()
    start = entry_text.find('{', m.end(1))
    if start == -1: return (entry_type, "unknown_key", {})
    i, depth, in_quotes, key_buf = start + 1, 0, False, []
    while i < len(entry_text):
        ch = entry_text[i]
        if ch == '"' and depth == 0: in_quotes = not in_quotes
        if not in_quotes and depth == 0 and ch == ',':
            i += 1; break
        key_buf.append(ch)
        if not in_quotes:
            if ch == '{': depth += 1
            elif ch == '}':
                if depth > 0: depth -= 1
        i += 1
    citation_key = ''.join(key_buf).strip()
    fields_text = entry_text[i:]
    fields, pos, L = {}, 0, len(fields_text)
    while pos < L:
        while pos < L and fields_text[pos].isspace(): pos += 1
        if pos >= L or fields_text[pos] == '}': break
        name_buf = []
        while pos < L and fields_text[pos] not in '=\n':
            if fields_text[pos] == '}': break
            name_buf.append(fields_text[pos]); pos += 1
        while pos < L and fields_text[pos] != '=':
            if fields_text[pos] == '}': break
            pos += 1
        if pos >= L or fields_text[pos] != '=': break
        pos += 1
        field_name = ''.join(name_buf).strip().lower().strip(',')
        while pos < L and fields_text[pos].isspace(): pos += 1
        val, pos = _parse_bib_value(fields_text, pos)
        if field_name: fields[field_name] = _clean_bib_value(val)
        while pos < L and fields_text[pos].isspace(): pos += 1
        if pos < L and fields_text[pos] == ',': pos += 1
    return (entry_type, citation_key, fields)

def infer_database(fields: Dict[str, str]) -> str:
    url = (fields.get('url', '') or fields.get('link', '')).strip()
    if not url: return 'ScienceDirect'
    m = re.search(r'://([^/]+)/', url)
    if m:
        host = m.group(1).lower()
        for k, v in DOMAIN_DB_MAP.items():
            if host == k or host.endswith('.' + k): return v
    return 'ScienceDirect'

def infer_type(entry_type: str) -> str:
    t = (entry_type or '').lower()
    if t == 'article': return 'artículo de revista'
    if t in ('inproceedings', 'conference', 'proceedings'): return 'artículo de congreso'
    if t in ('book',): return 'libro'
    if t in ('incollection', 'inbook', 'collection'): return 'capítulo de libro'
    if t in ('phdthesis', 'mastersthesis', 'thesis'): return 'tesis'
    if t in ('techreport', 'report'): return 'reporte técnico'
    return t or 'desconocido'

def extract_venue(entry_type: str, fields: Dict[str, str]) -> str:
    t = (entry_type or '').lower()
    if t == 'article': return fields.get('journal', '') or fields.get('journaltitle', '')
    if t in ('inproceedings', 'conference', 'proceedings', 'incollection', 'inbook'):
        return fields.get('booktitle', '')
    return fields.get('publisher', '') or fields.get('institution', '')

def normalize_doi(raw_doi: str) -> str:
    if not raw_doi: return ''
    import re
    doi = raw_doi.strip().replace('\\url{', '').replace('}', '')
    doi = re.sub(r'^(https?://(dx\.)?doi\.org/)', '', doi, flags=re.I)
    return doi.strip()

def parse_bib_folder(input_dir: str | Path):
    entries = []
    for root, dirs, files in os.walk(str(input_dir)):
        for fn in files:
            if fn.lower().endswith(('.bib', '.bibtex')):
                p = Path(root) / fn
                with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                    text = f.read()
                for raw in split_bibtex_entries(text):
                    etype, key, flds = parse_entry(raw)
                    if key and flds: entries.append((etype, key, flds))
    return entries

def entries_to_records(entries):
    records = []
    for etype, key, flds in entries:
        records.append({
            'ID': key,
            'DOI': normalize_doi(flds.get('doi', '')),
            'Titulo': flds.get('title', ''),
            'Año de publicacion fuente': flds.get('year', ''),
            'base de datos': infer_database(flds),
            'tipo de publicacion': infer_type(etype),
            'venue': extract_venue(etype, flds),
            'palabras clave': flds.get('keywords', ''),
            'abstract': flds.get('abstract', ''),
        })
    return records

def save_outputs(df: pd.DataFrame, out_csv: Path, out_xlsx: Path):
    df.to_csv(out_csv, index=False, encoding='utf-8')
    with pd.ExcelWriter(out_xlsx, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Resultados', index=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--input_dir', default='.', help='Folder containing .bib/.bibtex files (recursive)')
    ap.add_argument('--out_csv', default='bib_registros.csv')
    ap.add_argument('--out_xlsx', default='bib_registros.xlsx')
    args = ap.parse_args()

    entries = parse_bib_folder(args.input_dir)
    records = entries_to_records(entries)
    df = pd.DataFrame(records, columns=[
        'ID', 'DOI', 'Titulo', 'Año de publicacion fuente', 'base de datos',
        'tipo de publicacion', 'venue', 'palabras clave', 'abstract'
    ])
    save_outputs(df, Path(args.out_csv), Path(args.out_xlsx))
    print(f"Wrote {len(df)} rows to {args.out_csv} and {args.out_xlsx}")


"""
python bib2table.py --input_dir ./carpeta_con_bib --out_csv salida.csv --out_xlsx salida.xlsx

"""
if __name__ == '__main__':
    main()
