import pandas as pd

def transform_fact_web_session(raw_data, dims):
    ws = raw_data["web_session"].copy()
    dim_date = dims.get("dim_date", pd.DataFrame())[["date_sk", "date"]] if "dim_date" in dims else None

    ws = ws.rename(columns={"session_id": "session_id", "customer_id": "customer_id"})
    for col in ["started_at", "ended_at"]:
        if col in ws.columns:
            ws[col] = pd.to_datetime(ws[col], errors="coerce")

    if "started_at" in ws.columns and "ended_at" in ws.columns:
        ws["duration_minutes"] = (ws["ended_at"] - ws["started_at"]).dt.total_seconds() / 60.0

    if dim_date is not None and not dim_date.empty and "started_at" in ws.columns:
        ws = ws.merge(dim_date.rename(columns={"date": "started_at"}), how="left", on="started_at")
        ws = ws.rename(columns={"date_sk": "date_sk_start"})

    cols = ["session_id", "customer_id"]
    for c in ["device", "date_sk_start", "duration_minutes"]:
        if c in ws.columns: cols.append(c)
    return ws[cols]
