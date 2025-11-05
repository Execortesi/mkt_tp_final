import pandas as pd

def transform_fact_shipment(raw_data: dict, dims: dict) -> pd.DataFrame:
    sh = raw_data["shipment"].copy()
    out_cols = [
        "shipment_id",
        "order_id",
        "carrier",
        "tracking_number",
        "status",
        "shipped_at",
        "delivered_at",
    ]
    out_cols = [c for c in out_cols if c in sh.columns]

    return sh[out_cols].copy()

