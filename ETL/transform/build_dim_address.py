import pandas as pd

def build_dim_address(raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Cargamos address y province
    addr = raw_data["address"].copy()
    prov = raw_data["province"].copy()

    # Unimos para traer el nombre y código de provincia
    prov = prov.rename(columns={
        "province_id": "province_id",
        "name": "province_name",
        "code": "province_code"
    })
    addr = addr.merge(prov[["province_id", "province_name", "province_code"]],
                      how="left", on="province_id")

    # Clave sustituta
    addr.insert(0, "address_sk", range(1, len(addr) + 1))

    # Renombrar clave natural
    addr = addr.rename(columns={
        "address_id": "address_bk"
    })

    # Seleccionar columnas relevantes según el modelo del profe
    cols = [
        "address_sk", "address_bk", "line1", "line2", "city",
        "province_id", "province_code", "province_name",
        "postal_code", "country_code", "created_at"
    ]
    addr = addr[[c for c in cols if c in addr.columns]]

    return addr

