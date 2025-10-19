import argparse
import glob

from .readers.bib_reader import read_dir
"""
PYTHONPATH=src poetry run python -m src.main
"""

def args_init():
    ap = argparse.ArgumentParser()
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--filecvs", help="Path to one CSV file")
    group.add_argument("--pathcvs", help="Directory with cvs")
        
    ap.add_argument("--out_dir", required=True, help="Output directory for predictions")
    # ap.add_argument("--size", type=int, default=512, help="Resize used in training")
    # ap.add_argument("--in_ch", type=int, default=1)
    # ap.add_argument("--out_ch", type=int, default=2)
    # ap.add_argument("--string", type=str, default=None, help="If you used a custom NAS string")
    # ap.add_argument("--overlay", action="store_true", help="Save color overlay")
    # ap.add_argument("--overlay_alpha", type=float, default=0.35)
    # ap.add_argument("--overlay_color", type=str, default="0,0,255", help="B,G,R (OpenCV order)")
    args = ap.parse_args()

if __name__ == '__main__':
    #args_init()
    read_dir()


