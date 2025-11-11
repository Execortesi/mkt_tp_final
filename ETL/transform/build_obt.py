import pandas as pd

def _safe_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: list[str] | str | None = None,
    left_on: str | None = None,
    right_on: str | None = None,
    how: str = "left",
    suffixes: tuple[str, str] = ("", "_r"),
) -> pd.DataFrame:
    if left is None or right is None or len(right) == 0:
        return left

    if on is not None:
        keys = [on] if isinstance(on, str) else on
        for k in keys:
            if k not in left.columns or k not in right.columns:
                return left
        return left.merge(right, how=how, on=on, suffixes=suffixes)

    if left_on is not None and right_on is not None:
        if left_on not in left.columns or right_on not in right.columns:
            return left
        return left.merge(
            right, how=how, left_on=left_on, right_on=right_on, suffixes=suffixes
        )

    return left


def build_one_big_table(
    raw: dict[str, pd.DataFrame],
    dims: dict[str, pd.DataFrame],
    facts: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    One Big Table a nivel línea de pedido (sales_order_item).
    - Base: fact_sales_order_item
    - Enriquecida con fact_sales_order
    - Join con dim_date por order_date (fecha real)
    - Join con dim_product, dim_customer, dim_channel, dim_store, dim_address, dim_province
    - Campos de fecha listos para Looker y nombres de provincia (tienda / billing / shipping)
    """

    # ---------------------------
    # 1) Base: línea de pedido
    # ---------------------------
    soi = facts.get("fact_sales_order_item", pd.DataFrame()).copy()
    if soi.empty:
        raise ValueError("fact_order_item está vacío. Generá los facts primero.")

    for c in ["discount_amount", "line_total"]:
        if c not in soi.columns:
            soi[c] = 0.0

    # ---------------------------
    # 2) Cabecera del pedido
    # ---------------------------
    so = facts.get("fact_sales_order", pd.DataFrame()).copy()
    if so.empty:
        raise ValueError("fact_order está vacío. Generá los facts primero.")

    # Aseguramos tipo fecha
    if "order_date" in so.columns:
        so["order_date"] = pd.to_datetime(so["order_date"], errors="coerce")

    # Merge por order_id
    obt = _safe_merge(soi, so, on="order_id")

    # ---------------------------
    # 3) Fecha (dim_date) por order_date
    # ---------------------------
    dd = dims.get("dim_calendar", pd.DataFrame()).copy()
    if not dd.empty:
        # dim_date viene con 'date' (datetime) y todos los derivados
        dd = dd.rename(columns={"date": "order_date"})
        keep_date = [
            "order_date", "day", "month", "year", "month_name",
            "quarter", "week_number", "is_weekend", "year_month", "month_date"
        ]
        keep_date = [c for c in keep_date if c in dd.columns]

        # Aseguro order_date en OBT como datetime (por si no lo estaba)
        if "order_date" in obt.columns:
            obt["order_date"] = pd.to_datetime(obt["order_date"], errors="coerce")

        obt = _safe_merge(obt, dd[keep_date], left_on="order_date", right_on="order_date")

    else:
        # Fallback: genero derivados directo desde order_date
        if "order_date" in obt.columns:
            obt["order_date"] = pd.to_datetime(obt["order_date"], errors="coerce")
            obt["day"] = obt["order_date"].dt.day
            obt["month"] = obt["order_date"].dt.month
            obt["year"] = obt["order_date"].dt.year
            obt["quarter"] = obt["order_date"].dt.quarter
            obt["week_number"] = obt["order_date"].dt.isocalendar().week.astype(int)
            obt["is_weekend"] = obt["order_date"].dt.dayofweek >= 5
            meses_es = [
                "Enero","Febrero","Marzo","Abril","Mayo","Junio",
                "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
            ]
            obt["month_name"] = obt["month"].map(lambda m: meses_es[m-1])
            obt["year_month"] = obt["order_date"].dt.strftime("%Y-%m")
            obt["month_date"] = obt["order_date"].values.astype("datetime64[M]")

    # ---------------------------
    # 4) Producto (dim_product)
    # ---------------------------
    dp = dims.get("dim_product", pd.DataFrame()).copy()
    if not dp.empty:
        if "product_bk" in dp.columns:
            dp = dp.rename(columns={"product_bk": "product_id"})
        keep_prod = [
            "product_id", "sku", "name", "category_bk",
            "category_name", "category_parent_bk", "list_price"
        ]
        keep_prod = [c for c in keep_prod if c in dp.columns]
        obt = _safe_merge(obt, dp[keep_prod], on="product_id")

    # ---------------------------
    # 5) Cliente (dim_customer)
    # ---------------------------
    dc = dims.get("dim_customer", pd.DataFrame()).copy()
    if not dc.empty:
        if "customer_bk" in dc.columns:
            dc = dc.rename(columns={"customer_bk": "customer_id"})
        keep_cust = ["customer_id", "email", "first_name", "last_name", "phone"]
        keep_cust = [c for c in keep_cust if c in dc.columns]
        obt = _safe_merge(obt, dc[keep_cust], on="customer_id")

    # ---------------------------
    # 6) Canal (dim_channel)
    # ---------------------------
    ch = dims.get("dim_channel", pd.DataFrame()).copy()
    if not ch.empty:
        keep_ch = [c for c in ["channel_id", "code", "name"] if c in ch.columns]
        ch = ch[keep_ch].rename(columns={"code": "channel_code", "name": "channel_name"})
        obt = _safe_merge(obt, ch, on="channel_id")

    # ---------------------------
    # 7) Tienda (dim_store) + provincia de la tienda
    # ---------------------------
    ds = dims.get("dim_store", pd.DataFrame()).copy()
    if not ds.empty:
        if "store_bk" in ds.columns:
            ds = ds.rename(columns={"store_bk": "store_id"})
        keep_store = [
            "store_id","name","address_bk","city","province_id","postal_code","country_code"
        ]
        keep_store = [c for c in keep_store if c in ds.columns]
        ds = ds[keep_store].rename(columns={
            "name": "store_name",
            "address_bk": "store_address_id",
            "city": "store_city",
            "province_id": "store_province_id",
            "postal_code": "store_postal_code",
            "country_code": "store_country_code",
        })
        obt = _safe_merge(obt, ds, on="store_id")

    # ---------------------------
    # 8) Direcciones (billing / shipping) + provincias
    # ---------------------------
    da = dims.get("dim_address", pd.DataFrame()).copy()
    if not da.empty:
        keep_addr = ["address_bk", "line1", "line2", "city", "province_id", "postal_code", "country_code"]
        keep_addr = [c for c in keep_addr if c in da.columns]

        billing = da[keep_addr].rename(columns={
            "address_bk": "billing_address_id",
            "line1": "billing_line1",
            "line2": "billing_line2",
            "city": "billing_city",
            "province_id": "billing_province_id",
            "postal_code": "billing_postal_code",
            "country_code": "billing_country_code",
        })
        obt = _safe_merge(obt, billing, on="billing_address_id")

        shipping = da[keep_addr].rename(columns={
            "address_bk": "shipping_address_id",
            "line1": "shipping_line1",
            "line2": "shipping_line2",
            "city": "shipping_city",
            "province_id": "shipping_province_id",
            "postal_code": "shipping_postal_code",
            "country_code": "shipping_country_code",
        })
        obt = _safe_merge(obt, shipping, on="shipping_address_id")

    # Provincias (para tienda, billing y shipping)
    dpv = dims.get("dim_province", pd.DataFrame()).copy()
    if not dpv.empty:
        if "province_bk" in dpv.columns:
            dpv = dpv.rename(columns={"province_bk": "province_id"})
        keep_prov = [c for c in ["province_id", "name", "code"] if c in dpv.columns]
        dpv = dpv[keep_prov].rename(columns={"name": "province_name", "code": "province_code"})

        # tienda
        sp = dpv.rename(columns={
            "province_id":"store_province_id",
            "province_name":"store_province_name",
            "province_code":"store_province_code",
        })
        obt = _safe_merge(obt, sp, on="store_province_id")

        # billing
        bp = dpv.rename(columns={
            "province_id":"billing_province_id",
            "province_name":"billing_province_name",
            "province_code":"billing_province_code",
        })
        obt = _safe_merge(obt, bp, on="billing_province_id")

        # shipping
        shp = dpv.rename(columns={
            "province_id":"shipping_province_id",
            "province_name":"shipping_province_name",
            "province_code":"shipping_province_code",
        })
        obt = _safe_merge(obt, shp, on="shipping_province_id")

    # ---------------------------
    # 9) Métricas auxiliares
    # ---------------------------
    if "line_total" not in obt.columns:
        if {"quantity", "unit_price", "discount_amount"}.issubset(obt.columns):
            obt["line_total"] = obt["quantity"] * obt["unit_price"] - obt["discount_amount"]
        else:
            obt["line_total"] = 0.0

    # Ventas válidas (para KPIs)
    if "status" in obt.columns:
        obt["ventas_validas_line"] = obt.apply(
            lambda r: r["line_total"] if str(r["status"]).upper() in {"PAID", "FULFILLED"} else 0.0,
            axis=1
        )

    # ---------------------------
    # 10) Orden y limpieza final
    # ---------------------------
    final_cols_order = [
        # keys línea
        "order_item_id","order_id","product_id",
        # métricas línea
        "quantity","unit_price","discount_amount","line_total","ventas_validas_line",
        # cabecera pedido
        "customer_id","channel_id","channel_code","channel_name",
        "store_id","store_name",
        "order_date","day","month","year","month_name","quarter","week_number","is_weekend",
        "year_month","month_date",
        "billing_address_id","shipping_address_id",
        "status","currency_code","subtotal","tax_amount","shipping_fee","total_amount",
        # producto
        "sku","name","category_bk","category_name","category_parent_bk","list_price",
        # cliente
        "email","first_name","last_name","phone",
        # tienda + provincia tienda
        "store_address_id","store_city","store_province_id","store_province_name","store_province_code",
        "store_postal_code","store_country_code",
        # billing + provincia
        "billing_line1","billing_line2","billing_city","billing_postal_code","billing_country_code",
        "billing_province_id","billing_province_name","billing_province_code",
        # shipping + provincia
        "shipping_line1","shipping_line2","shipping_city","shipping_postal_code","shipping_country_code",
        "shipping_province_id","shipping_province_name","shipping_province_code",
    ]
    final_cols = [c for c in final_cols_order if c in obt.columns]
    obt = obt[final_cols].copy()

    return obt









