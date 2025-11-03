import pandas as pd

def transform_fact_shipment(raw_data, dims):
    sh = raw_data["shipment"].copy()
    dim_date = dims.get("dim_date", pd.DataFrame())[["date_sk", "date"]] if "dim_date" in dims else None

    sh = sh.rename(columns={"shipment_id": "shipment_id", "order_id": "order_id"})

    for col in ["shipped_at", "delivered_at"]:
        if col in sh.columns:
            sh[col] = pd.to_datetime(sh[col], errors="coerce")

    if dim_date is not None and not dim_date.empty:
        if "shipped_at" in sh.columns:
            sh = sh.merge(dim_date.rename(columns={"date": "shipped_at"}), how="left", on="shipped_at")
            sh = sh.rename(columns={"date_sk": "date_sk_shipped"})
        if "delivered_at" in sh.columns:
            sh = sh.merge(dim_date.rename(columns={"date": "delivered_at"}), how="left", on="delivered_at")
            # si hay conflicto de nombre, renombrá la segunda
            if "date_sk_y" in sh.columns:
                sh = sh.rename(columns={"date_sk_y": "date_sk_delivered"})
            elif "date_sk" in sh.columns and "date_sk_shipped" in sh.columns:
                sh = sh.rename(columns={"date_sk": "date_sk_delivered"})

    cols = ["shipment_id", "order_id"]
    for c in ["carrier", "tracking_number", "status", "date_sk_shipped", "date_sk_delivered"]:
        if c in sh.columns: cols.append(c)
    return sh[cols]
