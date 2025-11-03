import pandas as pd

def transform_fact_payment(raw_data, dims):
    pay = raw_data["payment"].copy()
    dim_date = dims.get("dim_date", pd.DataFrame())[["date_sk", "date"]] if "dim_date" in dims else None

    pay = pay.rename(columns={"payment_id": "payment_id", "order_id": "order_id"})
    if "paid_at" in pay.columns:
        pay["paid_at"] = pd.to_datetime(pay["paid_at"], errors="coerce")
        if dim_date is not None and not dim_date.empty:
            pay = pay.merge(dim_date.rename(columns={"date": "paid_at"}), how="left", on="paid_at")

    cols = ["payment_id", "order_id"]
    if "date_sk" in pay.columns: cols.append("date_sk")
    for c in ["amount", "status", "transaction_nr", "transaction_ref"]:
        if c in pay.columns: cols.append(c)
    return pay[cols]
