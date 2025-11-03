import pandas as pd
from pathlib import Path

def extract_raw_data(raw_dir: str = "raw") -> dict[str, pd.DataFrame]:
    """
    Lee todos los CSV dentro de la carpeta 'raw' y devuelve un diccionario
    con los DataFrames.
    """
    raw_path = Path(raw_dir)
    raw_data = {}

    for file_path in raw_path.glob("*.csv"):
        table_name = file_path.stem
        raw_data[table_name] = pd.read_csv(file_path)
        print(f" -> Archivo '{file_path.name}' cargado correctamente.")

    return raw_data

if __name__ == "__main__":
    datos_extraidos = extract_raw_data()
