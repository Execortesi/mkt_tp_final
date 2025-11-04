import pandas as pd

def transform_dim_customer(raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    cust = raw_data["customer"].copy()

    # Renombrar BK y crear surrogate key
    cust = cust.rename(columns={"customer_id": "customer_bk"})
    cust.insert(0, "customer_sk", range(1, len(cust) + 1))

    # Orden final alineado al PDF
    desired = [
        "customer_sk", "customer_bk",
        "email", "first_name", "last_name",
        "phone", "status", "created_at",
    ]
    cols = [c for c in desired if c in cust.columns]
    return cust[cols].drop_duplicates()

