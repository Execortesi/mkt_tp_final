# ETL/pipeline.py  (fragmento)
from ETL.extract.extract import extract_raw_data
from ETL.transform import transform_dimensions, transform_facts
from ETL.load.load import load_data_to_dw, save_one_big_table
from ETL.transform.build_obt import build_one_big_table  # <-- nuevo

RAW_DIR = "raw"
DW_DIR  = "DW"

def run_dimensions():
    raw = extract_raw_data(RAW_DIR)
    dims = transform_dimensions(raw)
    load_data_to_dw(dims, DW_DIR)
    print("✅ Dimensiones generadas.")

def run_facts():
    raw = extract_raw_data(RAW_DIR)
    dims = {}  # si quisieras leer dims del disco, acá podrías cargarlas
    facts = transform_facts(raw, dims)
    load_data_to_dw(facts, DW_DIR)
    print("✅ Hechos generados.")

def run_obt():
    raw = extract_raw_data(RAW_DIR)
    dims = transform_dimensions(raw)
    facts = transform_facts(raw, dims)
    obt = build_one_big_table(raw, dims, facts)
    save_one_big_table(obt, DW_DIR)
    print("✅ One Big Table generada.")

def run_all():
    run_dimensions()
    run_facts()
    run_obt()

