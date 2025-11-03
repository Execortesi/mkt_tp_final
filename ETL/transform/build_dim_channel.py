import pandas as pd

def build_dim_channel(raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    ch = raw_data["channel"].drop_duplicates().rename(columns={"channel_id": "channel_bk"})
    ch.insert(0, "channel_key", range(1, len(ch) + 1))
    return ch[["channel_key", "channel_bk", "code", "name"]]
