import pandas as pd

def transform_fact_payment(raw_data: dict, dims: dict) -> pd.DataFrame:
    pay = raw_data["payment"].copy()
    out_cols = [
        "payment_id",
        "order_id",
        "method",
        "status",
        "amount",
        "paid_at",
        "transaction_ref",
    ]
    out_cols = [c for c in out_cols if c in pay.columns]

    return pay[out_cols].copy()

