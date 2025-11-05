import pandas as pd

def _safe_merge(left: pd.DataFrame, right: pd.DataFrame, on: list[str] | str,
                how: str = "left", suffixes: tuple[str, str] = ("", "_r")) -> pd.DataFrame:
    if left is None or right is None or len(right) == 0:
        return left
    # asegurar que las keys existen en ambos lados
    if isinstance(on, str):
        keys = [on]
    else:
        keys = on
    for k in keys:
        if k not in left.columns or k not in right.columns:
            return left
    return left.merge(right, how=how, on=on, suffixes=suffixes)

def build_one_big_table(raw: dict[str, pd.DataFrame],
                        dims: dict[str, pd.DataFrame],
                        facts: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    One Big Table a nivel línea de pedido (sales_order_item):
    - Parte de fact_sales_order_item
    - Enriquece con cabecera (fact_sales_order)
    - Enriquece con dims: product, customer, store, address (billing/shipping), province, date, channel
    - Nombres compatibles con el PDF (BKs donde corresponde)
    """

    # ---------------------------
    # 1) Base: línea de pedido
    # ---------------------------
    soi = facts.get("fact_sales_order_item", pd.DataFrame()).copy()
    if soi.empty:
        raise ValueError("fact_sales_order_item está vacío. Generá los facts primero.")

    # columnas mínimas esperadas
    # order_item_id, order_id, product_id, quantity, unit_price, discount_amount, line_total
    for c in ["discount_amount", "line_total"]:
        if c not in soi.columns:
            soi[c] = 0

    # ---------------------------
    # 2) Cabecera de pedido
    # ---------------------------
    so = facts.get("fact_sales_order", pd.DataFrame()).copy()
    if so.empty:
        raise ValueError("fact_sales_order está vacío. Generá los facts primero.")

    # join por order_id (BK)
    obt = _safe_merge(soi, so, on="order_id")

    # ---------------------------
    # 3) Fecha del pedido (dim_date por order_date_id)
    # ---------------------------
    dd = dims.get("dim_date", pd.DataFrame()).copy()
    if not dd.empty:
        dd = dd.rename(columns={
            "date_sk": "order_date_id",
            "date": "order_date"
        })
        keep_date = ["order_date_id", "order_date", "day", "month", "year",
                     "day_name", "month_name", "quarter", "week_number", "year_month", "is_weekend"]
        keep_date = [c for c in keep_date if c in dd.columns]
        obt = _safe_merge(obt, dd[keep_date], on="order_date_id")

    # ---------------------------
    # 4) Producto (dim_product por product_id = product_bk)
    # ---------------------------
    dp = dims.get("dim_product", pd.DataFrame()).copy()
    if not dp.empty:
        dp = dp.rename(columns={
            "product_bk": "product_id"
        })
        keep_prod = ["product_id", "sku", "name", "category_bk", "category_name",
                     "category_parent_bk", "list_price", "status"]
        keep_prod = [c for c in keep_prod if c in dp.columns]
        obt = _safe_merge(obt, dp[keep_prod], on="product_id")

    # ---------------------------
    # 5) Cliente (dim_customer por customer_id = customer_bk)
    # ---------------------------
    dc = dims.get("dim_customer", pd.DataFrame()).copy()
    if not dc.empty:
        # tu dim_customer puede tener customer_bk como BK
        if "customer_bk" in dc.columns:
            dc = dc.rename(columns={"customer_bk": "customer_id"})
        keep_cust = ["customer_id", "email", "first_name", "last_name", "phone", "status", "created_at"]
        keep_cust = [c for c in keep_cust if c in dc.columns]
        obt = _safe_merge(obt, dc[keep_cust], on="customer_id")

    # ---------------------------
    # 6) Canal (dim_channel por channel_id)
    # ---------------------------
    ch = dims.get("dim_channel", pd.DataFrame()).copy()
    if not ch.empty and "channel_id" in ch.columns:
        keep_ch = ["channel_id", "code", "name"]
        keep_ch = [c for c in keep_ch if c in ch.columns]
        # renombramos para no pisar campos genéricos
        ch = ch[keep_ch].rename(columns={"name": "channel_name", "code": "channel_code"})
        obt = _safe_merge(obt, ch, on="channel_id")

    # ---------------------------
    # 7) Tienda (dim_store por store_id = store_bk)
    # ---------------------------
    ds = dims.get("dim_store", pd.DataFrame()).copy()
    if not ds.empty:
        if "store_bk" in ds.columns:
            ds = ds.rename(columns={"store_bk": "store_id"})
        keep_store = ["store_id", "name", "address_bk", "city", "province_id", "postal_code", "country_code"]
        keep_store = [c for c in keep_store if c in ds.columns]
        ds = ds[keep_store].rename(columns={"name": "store_name", "address_bk": "store_address_id"})
        obt = _safe_merge(obt, ds, on="store_id")

    # ---------------------------
    # 8) Direcciones (billing / shipping) + Provincia
    # ---------------------------
    da = dims.get("dim_address", pd.DataFrame()).copy()
    if not da.empty:
        # billing
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

        # shipping
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

        # Provincias
        dpv = dims.get("dim_province", pd.DataFrame()).copy()
        if not dpv.empty:
            # Asegurar BK
            if "province_bk" in dpv.columns:
                dpv = dpv.rename(columns={"province_bk": "province_id"})
            keep_prov = ["province_id", "name", "code"]
            keep_prov = [c for c in keep_prov if c in dpv.columns]
            dpv = dpv[keep_prov].rename(columns={
                "name": "province_name",
                "code": "province_code"
            })

            # attach billing province
            bp = dpv.rename(columns={
                "province_id": "billing_province_id",
                "province_name": "billing_province_name",
                "province_code": "billing_province_code",
            })
            obt = _safe_merge(obt, bp, on="billing_province_id")

            # attach shipping province
            sp = dpv.rename(columns={
                "province_id": "shipping_province_id",
                "province_name": "shipping_province_name",
                "province_code": "shipping_province_code",
            })
            obt = _safe_merge(obt, sp, on="shipping_province_id")

    # ---------------------------
    # 9) Derivadas útiles
    # ---------------------------
    if "line_total" not in obt.columns and {"quantity", "unit_price", "discount_amount"}.issubset(obt.columns):
        obt["line_total"] = obt["quantity"] * obt["unit_price"] - obt["discount_amount"]

    # ticket_total ≈ sumar por order_id en dashboards; acá mantenemos métricas a nivel línea

    # ---------------------------
    # 10) Orden y limpieza final
    # ---------------------------
    final_cols_order = [
        # keys de la línea
        "order_item_id", "order_id", "product_id",
        # métricas de la línea
        "quantity", "unit_price", "discount_amount", "line_total",
        # cabecera pedido
        "customer_id", "channel_id", "channel_code", "channel_name",
        "store_id", "store_name",
        "order_date_id", "order_date", "day", "month", "year", "year_month", "quarter", "week_number", "is_weekend",
        "billing_address_id", "shipping_address_id",
        "status", "currency_code", "subtotal", "tax_amount", "shipping_fee", "total_amount",
        # producto
        "sku", "name", "category_bk", "category_name", "category_parent_bk", "list_price",
        # cliente
        "email", "first_name", "last_name", "phone",
        # tienda/direcciones
        "store_address_id", "city", "province_id", "postal_code", "country_code",
        "billing_line1", "billing_line2", "billing_city", "billing_postal_code", "billing_country_code",
        "billing_province_id", "billing_province_name", "billing_province_code",
        "shipping_line1", "shipping_line2", "shipping_city", "shipping_postal_code", "shipping_country_code",
        "shipping_province_id", "shipping_province_name", "shipping_province_code",
    ]
    # conserva solo las que existen realmente
    final_cols = [c for c in final_cols_order if c in obt.columns]
    obt = obt[final_cols].copy()

    return obt





