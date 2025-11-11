# ETL/transform/build_fact_sales_order_item.py
import pandas as pd

REQ_COLS = [
    "order_item_id", "order_id", "product_id",
    "quantity", "unit_price", "discount_amount", "line_total"
]

def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def transform_fact_sales_order_item(raw_data: dict[str, pd.DataFrame],
                                    dims: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Construye fact_sales_order_item a partir de raw['sales_order_item'].

    Espera columnas:
      - order_item_id, order_id, product_id, quantity, unit_price, discount_amount, line_total
    """
    if "sales_order_item" not in raw_data:
        raise ValueError("No se encontró 'sales_order_item' en raw_data.")

    df = raw_data["sales_order_item"].copy()

    # Validación mínima de columnas
    missing = [c for c in REQ_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en sales_order_item: {missing}")

    # Tipos numéricos
    df = _coerce_numeric(
        df,
        ["quantity", "unit_price", "discount_amount", "line_total"]
    )

    # Completar nulos en discount_amount
    if "discount_amount" in df.columns:
        df["discount_amount"] = df["discount_amount"].fillna(0)

    # Calcular line_total cuando venga nulo o faltante
    if "line_total" not in df.columns:
        df["line_total"] = df["quantity"] * df["unit_price"] - df["discount_amount"]
    else:
        needs_calc = df["line_total"].isna()
        if needs_calc.any():
            df.loc[needs_calc, "line_total"] = (
                df.loc[needs_calc, "quantity"] * df.loc[needs_calc, "unit_price"]
                - df.loc[needs_calc, "discount_amount"]
            )

    # Orden/selección final (solo las columnas relevantes)
    keep = [
        "order_item_id", "order_id", "product_id",
        "quantity", "unit_price", "discount_amount", "line_total"
    ]
    df = df[keep].dropna(subset=["order_item_id", "order_id", "product_id"])

    # Asegurar que no quede vacío
    if df.empty:
        raise ValueError("fact_sales_order_item quedó vacío luego de limpiar; revisá los datos de raw/sales_order_item.csv.")

    return df.reset_index(drop=True)



