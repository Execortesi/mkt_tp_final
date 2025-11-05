import pandas as pd

def transform_fact_nps_response(raw_data: dict, dims: dict) -> pd.DataFrame:
    nps = raw_data["nps_response"].copy()

    if "comment" not in nps.columns:
        nps["comment"] = pd.NA

    out_cols = [
        "nps_id",
        "customer_id",
        "channel_id",
        "score",
        "comment",
        "responded_at",
    ]
    out_cols = [c for c in out_cols if c in nps.columns]

    if "responded_at" in nps.columns:
        nps["responded_at"] = pd.to_datetime(nps["responded_at"], errors="coerce")

    return nps[out_cols].copy()


