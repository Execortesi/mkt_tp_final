import pandas as pd

def transform_dim_product(raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Bases
    prod = raw_data["product"].copy()
    cat  = raw_data.get("product_category", pd.DataFrame()).copy()

    # Renombres a BK (business keys) y campos base
    prod = prod.rename(columns={
        "product_id": "product_bk",
        "category_id": "category_bk",
    })

    # Surrogate key
    prod.insert(0, "product_sk", range(1, len(prod) + 1))

    # Traemos metadata de categoría si existe
    if not cat.empty:
        cat = cat.rename(columns={
            "category_id": "category_bk",
            "name": "category_name",
            "parent_id": "category_parent_bk",
        })
        cat = cat[["category_bk", "category_name", "category_parent_bk"]]
        prod = prod.merge(cat, how="left", on="category_bk")

    # Orden final de columnas (según PDF + extras útiles)
    desired = [
        "product_sk", "product_bk",
        "sku", "name",
        "category_bk", "category_name", "category_parent_bk",
        "list_price", "status", "created_at"
    ]
    cols = [c for c in desired if c in prod.columns]
    return prod[cols].drop_duplicates()
