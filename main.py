# main.py (reemplazá el contenido por esto si querés simple y claro)
import argparse
from ETL.pipeline import run_all, run_dimensions, run_facts, run_obt

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=["dims", "facts", "obt", "all"], default="all")
    args = parser.parse_args()

    {"dims": run_dimensions,
     "facts": run_facts,
     "obt":  run_obt,
     "all":  run_all}[args.step]()