import pandas as pd

def _safe_merge(left: pd.DataFrame, right: pd.DataFrame, on, how="left", suffixes=("", "_r")) -> pd.DataFrame:
    if left is None or right is None or len(right) == 0:
        return left
    keys = [on] if isinstance(on, str) else list(on)
    for k in keys:
        if k not in left.columns or k not in right.columns:
            return left
    return left.merge(right, how=how, on=on, suffixes=suffixes)

def build_one_big_table(raw: dict[str, pd.DataFrame],
                        dims: dict[str, pd.DataFrame],
                        facts: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # ========= 1) Base: línea de pedido =========
    soi = facts.get("fact_sales_order_item", pd.DataFrame()).copy()
    if soi.empty:
        raise ValueError("fact_order_item está vacío. Generá los facts primero.")

    for c in ["discount_amount", "line_total"]:
        if c not in soi.columns:
            soi[c] = 0

    # ========= 2) Cabecera de pedido =========
    so = facts.get("fact_sales_order", pd.DataFrame()).copy()
    if so.empty:
        raise ValueError("fact_sales_order está vacío. Generá los facts primero.")

    obt = _safe_merge(soi, so, on="order_id")

        # ========= 3) Derivados de fecha DESDE order_date (sin dim nueva) =========
    # 3.a) Asegurar que exista order_date: si no está en el merge con fact_sales_order,
    #      lo traemos directo del RAW 'sales_order.csv' por order_id.
    if "order_date" not in obt.columns or obt["order_date"].isna().all():
        so_raw = raw.get("sales_order", pd.DataFrame()).copy()
        if not so_raw.empty and "order_id" in so_raw.columns:
            # normalizamos posible nombre de fecha en el raw
            # intenta detectar 'order_date' o variantes comunes
            cand = [c for c in so_raw.columns if c.lower() in {"order_date", "orderdate", "date"}]
            if cand:
                so_raw = so_raw[["order_id", cand[0]]].rename(columns={cand[0]: "order_date"})
                obt = _safe_merge(obt, so_raw, on="order_id")

    # 3.b) Si aún no hay order_date pero existen year/month/day, la construimos
    if ("order_date" not in obt.columns or obt["order_date"].isna().all()) and \
       {"year", "month", "day"}.issubset(obt.columns):
        obt["order_date"] = pd.to_datetime(
            obt[["year","month","day"]].rename(columns={"year":"Y","month":"M","day":"D"})
            .assign(Y=lambda x: x["Y"].astype("Int64"),
                    M=lambda x: x["M"].astype("Int64"),
                    D=lambda x: x["D"].astype("Int64")),
            errors="coerce"
        )

    if "order_date" in obt.columns:
        d = pd.to_datetime(obt["order_date"], errors="coerce")
        if d.notna().any():
            # derivadas ANTES de formatear
            obt["day"]        = d.dt.day
            obt["month"]      = d.dt.month
            obt["year"]       = d.dt.year
            obt["year_month"] = d.dt.strftime("%Y-%m")
            obt["quarter"]    = d.dt.quarter
            try:
                obt["week_number"] = d.dt.isocalendar().week.astype("Int64")
            except Exception:
                obt["week_number"] = d.dt.week
            obt["is_weekend"] = (d.dt.dayofweek >= 5)

            # normalización FINAL para Looker: YYYY-MM-DD (tipo fecha reconocible)
            obt["order_date"] = d.dt.strftime("%Y-%m-%d")


    # ========= 4) Producto =========
    dp = dims.get("dim_product", pd.DataFrame()).copy()
    if not dp.empty:
        if "product_bk" in dp.columns and "product_id" not in dp.columns:
            dp = dp.rename(columns={"product_bk": "product_id"})
        keep_prod = [c for c in ["product_id","sku","name","category_bk","category_name","category_parent_bk","list_price","status"] if c in dp.columns]
        obt = _safe_merge(obt, dp[keep_prod], on="product_id")

    # ========= 5) Cliente =========
    dc = dims.get("dim_customer", pd.DataFrame()).copy()
    if not dc.empty:
        if "customer_bk" in dc.columns and "customer_id" not in dc.columns:
            dc = dc.rename(columns={"customer_bk": "customer_id"})
        keep_cust = [c for c in ["customer_id","email","first_name","last_name","phone","status","created_at"] if c in dc.columns]
        obt = _safe_merge(obt, dc[keep_cust], on="customer_id")

    # ========= 6) Canal =========
    ch = dims.get("dim_channel", pd.DataFrame()).copy()
    if not ch.empty and "channel_id" in ch.columns:
        keep_ch = [c for c in ["channel_id","code","name"] if c in ch.columns]
        ch = ch[keep_ch].rename(columns={"name":"channel_name","code":"channel_code"})
        obt = _safe_merge(obt, ch, on="channel_id")

    # ========= 7) Tienda =========
    ds = dims.get("dim_store", pd.DataFrame()).copy()
    if not ds.empty:
        if "store_bk" in ds.columns and "store_id" not in ds.columns:
            ds = ds.rename(columns={"store_bk": "store_id"})
        keep_store = [c for c in ["store_id","name","address_bk","city","province_id","postal_code","country_code"] if c in ds.columns]
        ds = ds[keep_store].rename(columns={
            "name":"store_name",
            "address_bk":"store_address_id",
            "city":"store_city",
            "province_id":"store_province_id",
            "postal_code":"store_postal_code",
            "country_code":"store_country_code",
        })
        obt = _safe_merge(obt, ds, on="store_id")

    # ========= 8) Direcciones (billing/shipping) =========
    da = dims.get("dim_address", pd.DataFrame()).copy()
    if not da.empty:
        keep_addr = [c for c in ["address_bk","line1","line2","city","province_id","postal_code","country_code"] if c in da.columns]

        billing = da[keep_addr].rename(columns={
            "address_bk":"billing_address_id",
            "line1":"billing_line1",
            "line2":"billing_line2",
            "city":"billing_city",
            "province_id":"billing_province_id",
            "postal_code":"billing_postal_code",
            "country_code":"billing_country_code",
        })
        obt = _safe_merge(obt, billing, on="billing_address_id")

        shipping = da[keep_addr].rename(columns={
            "address_bk":"shipping_address_id",
            "line1":"shipping_line1",
            "line2":"shipping_line2",
            "city":"shipping_city",
            "province_id":"shipping_province_id",
            "postal_code":"shipping_postal_code",
            "country_code":"shipping_country_code",
        })
        obt = _safe_merge(obt, shipping, on="shipping_address_id")

    # ========= 9) Provincias: tomar DIRECTO de raw['province'] (sin dim nueva) =========
    prov = raw.get("province", pd.DataFrame()).copy()
    if not prov.empty:
        base = [c for c in ["province_id","name","code"] if c in prov.columns]
        if base:
            # store
            sprov = prov[base].rename(columns={
                "province_id":"store_province_id",
                "name":"store_province_name",
                "code":"store_province_code",
            })
            obt = _safe_merge(obt, sprov, on="store_province_id")
            # billing
            bprov = prov[base].rename(columns={
                "province_id":"billing_province_id",
                "name":"billing_province_name",
                "code":"billing_province_code",
            })
            obt = _safe_merge(obt, bprov, on="billing_province_id")
            # shipping
            shprov = prov[base].rename(columns={
                "province_id":"shipping_province_id",
                "name":"shipping_province_name",
                "code":"shipping_province_code",
            })
            obt = _safe_merge(obt, shprov, on="shipping_province_id")

    # ========= 10) Métrica de línea por si faltara =========
    if "line_total" not in obt.columns and {"quantity","unit_price","discount_amount"}.issubset(obt.columns):
        obt["line_total"] = obt["quantity"] * obt["unit_price"] - obt["discount_amount"]
    
    if "ventas_validas_line" not in obt.columns:
        cond = obt.get("status").isin(["PAID", "FULFILLED"]) if "status" in obt.columns else False
        obt["ventas_validas_line"] = obt["line_total"].where(cond, 0)

    # ========= 11) Orden final =========
    final_cols_order = [
        "order_item_id","order_id","product_id",
        "quantity","unit_price","discount_amount","line_total","ventas_validas_line",
        "customer_id","channel_id","channel_code","channel_name",
        "store_id","store_name","store_address_id","store_city",
        "store_province_id","store_province_name","store_province_code","store_postal_code","store_country_code",
        "billing_address_id","billing_line1","billing_line2","billing_city",
        "billing_province_id","billing_province_name","billing_province_code","billing_postal_code","billing_country_code",
        "shipping_address_id","shipping_line1","shipping_line2","shipping_city",
        "shipping_province_id","shipping_province_name","shipping_province_code","shipping_postal_code","shipping_country_code",
        "status","currency_code","subtotal","tax_amount","shipping_fee","total_amount",
        # fechas derivadas (sin dim)
        "order_date","day","month","year","year_month","quarter","week_number","is_weekend",
        # producto/cliente
        "sku","name","category_bk","category_name","category_parent_bk","list_price",
        "email","first_name","last_name","phone",
    ]
    final_cols = [c for c in final_cols_order if c in obt.columns]
    return obt[final_cols].copy()










