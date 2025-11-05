import pandas as pd

def transform_fact_web_session(raw_data: dict, dims: dict) -> pd.DataFrame:
    ws = raw_data["web_session"].copy()
    out_cols = [
        "session_id",
        "customer_id",
        "started_at",
        "ended_at",
        "source",
        "device",
    ]
    out_cols = [c for c in out_cols if c in ws.columns]  # tolerante a columnas faltantes

    return ws[out_cols].copy()

