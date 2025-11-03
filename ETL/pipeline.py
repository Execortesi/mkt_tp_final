# ETL/pipeline.py
from ETL.extract.extract import extract_raw_data
from ETL.transform import transform_dimensions, transform_facts
from ETL.load.load import load_data_to_dw

RAW_DIR = "raw"
DW_DIR  = "DW"

def run_dimensions():
    raw  = extract_raw_data(RAW_DIR)
    dims = transform_dimensions(raw)
    load_data_to_dw(dims, DW_DIR)    
    print("✅ Dimensiones generadas.")

def run_facts():
    raw  = extract_raw_data(RAW_DIR)
    dims = transform_dimensions(raw)

    facts = transform_facts(raw, dims)
    load_data_to_dw(facts, DW_DIR)   
    print("✅ Hechos generados (si había).")

def run_all():
    run_dimensions()
    run_facts()
