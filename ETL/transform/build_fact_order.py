import pandas as pd

def _pick_cols(df, sk_candidates, bk_candidates):
    cols = set(df.columns)
    sk = next((c for c in sk_candidates if c in cols), None)
    bk = next((c for c in bk_candidates if c in cols), None)
    return sk, bk

def transform_fact_order(raw_data, dims):
    so = raw_data["sales_order"].copy()


    dim_customer = dims.get("dim_customer")
    if dim_customer is not None:
        c_sk, c_bk = _pick_cols(dim_customer, ["customer_sk", "cust_sk", "customer_key"],
                                ["customer_id", "cust_id", "id", "customer_bk"])
        if c_sk and c_bk and c_bk in so.columns:
            so = so.merge(
                dim_customer[[c_sk, c_bk]].rename(columns={c_sk: "customer_sk", c_bk: "customer_id"}),
                how="left",
                on="customer_id"
            )

    dim_channel = dims.get("dim_channel")
    if dim_channel is not None and "channel_id" in so.columns:
        ch_sk, ch_bk = _pick_cols(dim_channel,
                                  ["channel_sk", "chan_sk", "channel_key"],
                                  ["channel_id", "chan_id", "id", "channel_bk"])
        if ch_sk and ch_bk:
            so = so.merge(
                dim_channel[[ch_sk, ch_bk]],
                how="left",
                left_on="channel_id",   
                right_on=ch_bk          
            )
            so = so.rename(columns={ch_sk: "channel_sk"})
            if ch_bk in so.columns:
                so = so.drop(columns=[ch_bk])

    dim_store = dims.get("dim_store")
    if dim_store is not None and "store_id" in so.columns:
        s_sk, s_bk = _pick_cols(dim_store, ["store_sk", "store_key"], ["store_id", "id", "store_bk"])
        if s_sk and s_bk:
            so = so.merge(
                dim_store[[s_sk, s_bk]],
                how="left",
                left_on="store_id",
                right_on=s_bk
            )
            so = so.rename(columns={s_sk: "store_sk"})
            if s_bk in so.columns:
                so = so.drop(columns=[s_bk])

    dim_date = dims.get("dim_date")
    if dim_date is not None and "order_date" in so.columns:
        so["order_date"] = pd.to_datetime(so["order_date"], errors="coerce")
        if "date" in dim_date.columns and "date_sk" in dim_date.columns:
            so = so.merge(
                dim_date.rename(columns={"date": "order_date"}),
                how="left",
                on="order_date"
            )

    measures = [c for c in ["total_amount", "shipping_amount", "tax_amount", "final_amount"] if c in so.columns]

    cols = ["order_id"]
    if "date_sk"     in so.columns: cols.append("date_sk")
    if "customer_sk" in so.columns: cols.append("customer_sk")
    if "channel_sk"  in so.columns: cols.append("channel_sk")
    if "store_sk"    in so.columns: cols.append("store_sk")
    if "status"      in so.columns: cols.append("status")
    cols += measures
    cols = [c for c in cols if c in so.columns]
    return so[cols]

