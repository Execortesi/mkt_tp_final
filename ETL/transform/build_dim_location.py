import pandas as pd

def transform_dim_location(raw_data):
    addr = raw_data["address"].copy()
    prov = raw_data.get("province", pd.DataFrame()).copy()

    # surrogate key
    addr.insert(0, "location_sk", range(1, 1 + len(addr)))

    # join con provincia (si existe)
    if not prov.empty:
        prov = prov.rename(columns={"name": "province_name", "code": "province_code"})
        addr = addr.merge(prov[["province_id", "province_name", "province_code"]],
                          how="left", on="province_id")

    # renombrar BK
    addr = addr.rename(columns={"address_id": "location_id"})

    cols = [
        "location_sk", "location_id",
        "line1", "line2", "province_id", "province_code", "province_name",
        "postal_code", "country_code", "created_at"
    ]
    cols = [c for c in cols if c in addr.columns]
    return addr[cols]