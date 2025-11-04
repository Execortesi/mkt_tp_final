import pandas as pd

def transform_dim_store(raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Base store
    st = raw_data["store"].copy()
    st = st.rename(columns={
        "store_id": "store_bk",
        "address_id": "address_bk",
    })
    # Surrogate key
    st.insert(0, "store_sk", range(1, len(st) + 1))

    # Enriquecemos con datos de address (opcional pero útil)
    addr = raw_data.get("address")
    if addr is not None:
        keep_addr = ["address_id", "city", "province_id", "postal_code", "country_code"]
        keep_addr = [c for c in keep_addr if c in addr.columns]
        st = st.merge(addr[keep_addr],
                      how="left",
                      left_on="address_bk",
                      right_on="address_id")
        st = st.drop(columns=[c for c in ["address_id"] if c in st.columns])

    # Orden de columnas (mínimo + extra si existen)
    cols = ["store_sk", "store_bk", "name", "address_bk",
            "city", "province_id", "postal_code", "country_code"]
    cols = [c for c in cols if c in st.columns]
    return st[cols]

