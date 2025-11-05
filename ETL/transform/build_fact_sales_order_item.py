# ETL/transform/build_fact_order_item.py
import pandas as pd

def transform_fact_sales_order_item(raw_data: dict, dims: dict) -> pd.DataFrame:
    """
    Fact: sales_order_item (según PDF)
    No hacemos joins: usamos product_id directamente.
    """
    soi = raw_data["sales_order_item"].copy()

    if "discount_amount" not in soi.columns:
        soi["discount_amount"] = 0

    out_cols = [
        "order_item_id",
        "order_id",
        "product_id",      # <- se mantiene como en el PDF
        "quantity",
        "unit_price",
        "discount_amount",
        "line_total",
    ]
    out_cols = [c for c in out_cols if c in soi.columns]
    return soi[out_cols].copy()

