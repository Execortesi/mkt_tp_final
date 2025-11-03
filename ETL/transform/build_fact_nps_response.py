import pandas as pd

def _first_present(df, candidates):
    cols = set(df.columns)
    return next((c for c in candidates if c in cols), None)

def transform_fact_nps_response(raw_data, dims):
    nps = raw_data["nps_response"].copy()

    id_col   = _first_present(nps, ["response_id", "nps_id", "id"])
    date_col = _first_present(nps, ["response_date", "responded_at", "created_at"])

    if id_col and id_col != "response_id":
        nps = nps.rename(columns={id_col: "response_id"})
    elif not id_col:
        nps.insert(0, "response_id", range(1, 1 + len(nps)))

    if date_col:
        if date_col != "response_date":
            nps = nps.rename(columns={date_col: "response_date"})
        nps["response_date"] = pd.to_datetime(nps["response_date"], errors="coerce")

    dim_channel = dims.get("dim_channel")
    if dim_channel is not None and "channel_id" in nps.columns:
        if {"channel_key", "channel_bk"}.issubset(dim_channel.columns):
            nps = nps.merge(
                dim_channel[["channel_key", "channel_bk"]],
                how="left",
                left_on="channel_id",   # FK en fact
                right_on="channel_bk"   # BK en dim
            )
            nps = nps.rename(columns={"channel_key": "channel_sk"})
            nps = nps.drop(columns=["channel_bk"], errors="ignore")

    cols = ["response_id"]
    for c in ["channel_sk", "customer_id", "score", "response_date"]:
        if c in nps.columns:
            cols.append(c)

    return nps[cols]

