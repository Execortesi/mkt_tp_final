import pandas as pd

def build_dim_channel(raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Tomamos la tabla cruda
    ch = raw_data["channel"].drop_duplicates()

    # Renombramos el campo natural del sistema a channel_id
    # (antes lo llamabas channel_bk)
    ch = ch.rename(columns={"channel_id": "channel_id"})

    # Nos quedamos solo con las columnas que pide el profe
    ch = ch[["channel_id", "code", "name"]]

    return ch

