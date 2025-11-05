# ETL/transform/build_fact_sales_order.py
import pandas as pd

def transform_fact_sales_order(raw_data: dict, dims: dict) -> pd.DataFrame:
    """
    Fact: fact_sales_order (según PDF del profe)
    - order_date_id referencia a dim_date.date_sk
    - El resto de columnas salen directo de raw.sales_order
    """
    so = raw_data["sales_order"].copy()

    # Asegurar tipos de fecha
    so["order_date"] = pd.to_datetime(so["order_date"])

    dd = dims["dim_date"][["date_sk", "date"]].copy()
    dd["date"] = pd.to_datetime(dd["date"])

    # Mapear order_date -> order_date_id (date_sk)
    so = so.merge(
        dd.rename(columns={"date": "order_date", "date_sk": "order_date_id"}),
        how="left",
        on="order_date",
    )

    # Columnas según el enunciado del profe
    out_cols = [
        "order_id",
        "customer_id",
        "channel_id",
        "store_id",
        "order_date_id",       # <- FK a dim_date
        "billing_address_id",
        "shipping_address_id",
        "status",
        "currency_code",
        "subtotal",
        "tax_amount",
        "shipping_fee",
        "total_amount",
    ]

    return so[out_cols].copy()








