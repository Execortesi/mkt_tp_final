import pandas as pd

def transform_dim_store(raw_data):
    store = raw_data["store"].copy()

    # surrogate key
    store.insert(0, "store_sk", range(1, 1 + len(store)))

    cols = ["store_sk", "store_id", "name", "address_id", "active", "created_at"]
    cols = [c for c in cols if c in store.columns]
    return store[cols]
