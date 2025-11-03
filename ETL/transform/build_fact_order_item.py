import pandas as pd

def transform_fact_order_item(raw_data, dims):
    oi = raw_data["sales_order_item"].copy()
    dim_product = dims["dim_product"][["product_sk", "product_id"]]

    oi = oi.rename(columns={"product_id": "product_id", "order_id": "order_id"})
    oi = oi.merge(dim_product, how="left", on="product_id")

    if "quantity" in oi.columns and "unit_price" in oi.columns:
        oi["line_total"] = oi["quantity"] * oi["unit_price"]

    cols = ["order_id", "product_sk"]
    for c in ["order_item_id", "quantity", "unit_price", "line_total"]:
        if c in oi.columns: cols.append(c)

    return oi[cols]
