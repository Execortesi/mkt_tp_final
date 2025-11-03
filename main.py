import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))

from ETL.pipeline import run_all, run_dimensions, run_facts
import argparse

def parse_args():
    p = argparse.ArgumentParser(description="Orquestador ETL - TP Final")
    p.add_argument("--step", choices=["all","dims","facts"], default="all")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    {"dims": run_dimensions, "facts": run_facts}.get(args.step, run_all)()