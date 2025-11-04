from pathlib import Path
import pandas as pd  # solo para tipado; no es obligatorio

def load_data_to_dw(transformed_data: dict[str, pd.DataFrame], dw_dir: str = "DW") -> None:
    """
    Guarda cada DataFrame del diccionario 'transformed_data' en la carpeta DW,
    usando como nombre de archivo la clave del diccionario + '.csv'.
    """
    print("\n--- 🚚 Iniciando carga a DW ---")
    dw_path = Path(dw_dir)
    dw_path.mkdir(exist_ok=True)

    for table_name, df in transformed_data.items():
        out_path = dw_path / f"{table_name}.csv"
        df.to_csv(out_path, index=False)
        print(f" -> Tabla '{out_path.name}' guardada.")

    print("--- ✅ Carga completada ---\n")

def save_one_big_table(df, output_dir):
    out = Path(output_dir) / "one_big_table.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8")
    print("-> Tabla 'one_big_table.csv' guardada.")
