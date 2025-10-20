import argparse
import os
import sys
from .readers.bib_reader import normalize_bib_dir, dump_ndjson, read_bib_file



def ensure_parent_dir(path: str):
    parent = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(parent, exist_ok=True)

def build_parser():
    p = argparse.ArgumentParser(prog="dataset-tool", description="Process datasets (BibTeX/CSV).")
    sub = p.add_subparsers(dest="cmd", required=True)

    # bib subcommand
    pb = sub.add_parser("bib", help="Process BibTeX inputs")
    gb = pb.add_mutually_exclusive_group(required=True)
    gb.add_argument("--dir", dest="in_dir", help="Directory with .bib files")
    gb.add_argument("--file", dest="in_file", help="Single .bib file")
    pb.add_argument("--out", required=True, help="Output NDJSON file")

    # csv subcommand (placeholder)
    pcsv = sub.add_parser("csv", help="Process CSV inputs")
    gc = pcsv.add_mutually_exclusive_group(required=True)
    gc.add_argument("--dir", dest="in_dir", help="Directory with .csv files")
    gc.add_argument("--file", dest="in_file", help="Single .csv file")
    pcsv.add_argument("--out", required=True, help="Output file (TBD)")

    return p

def main():
    parser = build_parser()
    args = parser.parse_args()
    if args.cmd == "bib":
        if args.in_dir:
            if not os.path.isdir(args.in_dir):
                print(f"[ERROR] Directory not found: {args.in_dir}", file=sys.stderr)
                sys.exit(2)
            entries = normalize_bib_dir(args.in_dir)
        else:
            if not os.path.isfile(args.in_file):
                print(f"[ERROR] File not found: {args.in_file}", file=sys.stderr)
                sys.exit(2)
            entries = read_bib_file(args.in_file)

        ensure_parent_dir(args.out)
        dump_ndjson(entries, args.out)
        print(f"Saved {len(entries)} entries -> {args.out}")

    elif args.cmd == "csv":
        print("[WARN] CSV flow not implemented yet.", file=sys.stderr)
        sys.exit(4)

"""
PYTHONPATH=src poetry run python -m src.main \
    bib \
    --dir /home/ivan/Downloads/cadenas/input/sciencedirect \
    --out /home/ivan/Downloads/cadenas/output/entries.ndjson \
    

"""

if __name__ == "__main__":
    main()
