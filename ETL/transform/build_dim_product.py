import pandas as pd

def transform_dim_product(raw_data):
    prod = raw_data["product"].copy()
    prod_cat = raw_data.get("product_category", pd.DataFrame()).copy()

    prod.insert(0, "product_sk", range(1, 1 + len(prod)))

    if not prod_cat.empty:
        prod_cat = prod_cat.rename(columns={
            "name": "category_name",
            "category_id": "category_id",
            "parent_id": "category_parent_id"
        })
        prod = prod.merge(
            prod_cat[["category_id", "category_name", "category_parent_id"]],
            how="left", on="category_id"
        )

    cols = [
        "product_sk", "product_id", "sku", "name",
        "list_price", "status", "created_at",
        "category_id", "category_name", "category_parent_id"
    ]
    cols = [c for c in cols if c in prod.columns]
    return prod[cols]