# ETL/transform/build_obt.py
import pandas as pd

def build_one_big_table(raw_data: dict, dims: dict, facts: dict) -> pd.DataFrame:
    """
    Arma una OBT (one-big-table) para Looker Studio con lo mínimo pedido:
    - Fecha (date, year, month)
    - Canal (code, name)
    - Provincia (desde store -> address/location)
    - Producto (name, category_name)
    - Métricas: quantity, unit_price, line_total, total_amount, tax_amount, payment_amount
    - Otras: order_id, status, store_name, customer_sk, nps_score (si existe)
    """

    # -------------------------
    # Dimensiones necesarias
    # -------------------------
    # Fecha (tiene: date_sk, date, year, month, day, week, dow, ym)
    dd = dims["dim_date"][["date_sk", "date", "year", "month"]].copy()

    # Canal: en DW/dim_channel.csv hay: channel_key, channel_bk, code, name
    dch = dims["dim_channel"][["channel_key", "code", "name"]].copy()
    dch = dch.rename(columns={
        "channel_key": "channel_sk",
        "code": "channel_code",
        "name": "channel_name",
    })

    # Tiendas + ubicación: DW/dim_store.csv (store_sk, store_id, name, address_id)
    # y DW/dim_location.csv (location_sk, location_id, ..., province_name, ...)
    dst = dims["dim_store"][["store_sk", "name", "address_id"]].copy()
    dst = dst.rename(columns={"name": "store_name"})

    dloc = dims["dim_location"][["location_id", "province_name"]].copy()
    # join para traer provincia a la tienda
    dst = dst.merge(dloc, how="left", left_on="address_id", right_on="location_id")

    # Productos (para nombre y categoría)
    dprod = dims["dim_product"][["product_sk", "name", "category_name"]].copy()
    dprod = dprod.rename(columns={"name": "product_name"})

    # Customers (para exponer customer_sk en la OBT)
    # DW/dim_customer.csv: customer_sk, customer_id, ...
    dcust = dims["dim_customer"][["customer_sk", "customer_id"]].copy()

    # -------------------------
    # Hechos
    # -------------------------
    # Orden (DW/fact_order.csv):
    #   order_id, date_sk, customer_sk, channel_sk, store_sk, status, total_amount, tax_amount
    f_order = facts["fact_order"].copy()

    # Items (DW/fact_order_item.csv):
    #   order_id, product_sk, order_item_id, quantity, unit_price, line_total
    f_item = facts["fact_order_item"].copy()

    # Pagos (DW/fact_payment.csv):
    #   payment_id, order_id, date_sk, amount, status, transaction_ref
    f_pay = facts["fact_payment"][["order_id", "amount"]].copy()
    f_pay = f_pay.groupby("order_id", as_index=False)["amount"].sum()
    f_pay = f_pay.rename(columns={"amount": "payment_amount"})

    # NPS (si existe): fact_nps_response.csv
    #   response_id, channel_sk, customer_id, score, response_date
    # Lo llevamos a nivel customer_sk (mapeando por customer_id con dim_customer)
    nps_score_by_cust = None
    if "fact_nps_response" in facts:
        fnps = facts["fact_nps_response"][["customer_id", "score"]].copy()
        # mapeo customer_id -> customer_sk
        fnps = fnps.merge(dcust, how="left", on="customer_id")
        nps_score_by_cust = (
            fnps.dropna(subset=["customer_sk"])
                .groupby("customer_sk", as_index=False)["score"]
                .mean()
                .rename(columns={"score": "nps_score"})
        )

    # -------------------------
    # Construcción de la OBT
    # -------------------------
    # Base: fact_order + fecha + canal + tienda/provincia
    obt = (
        f_order
        # fecha
        .merge(dd, how="left", on="date_sk")
        # canal
        .merge(dch, how="left", on="channel_sk")
        # tienda + provincia
        .merge(dst[["store_sk", "store_name", "province_name"]], how="left", on="store_sk")
    )

    # Agrego métricas por ítem y producto
    obt = (
        obt
        .merge(
            f_item.merge(dprod, how="left", on="product_sk"),
            how="left",
            on="order_id"
        )
    )

    # Agrego pagos (sumados por order_id)
    obt = obt.merge(f_pay, how="left", on="order_id")

    # Agrego NPS por customer_sk (si lo calculamos)
    if nps_score_by_cust is not None:
        obt = obt.merge(nps_score_by_cust, how="left", on="customer_sk")
    else:
        obt["nps_score"] = pd.NA

    # Selección/orden final de columnas (todas existen con tus headers)
    final_cols = [
        "order_id",
        "date_sk", "date", "year", "month",
        "channel_code", "channel_name",
        "province_name",
        "product_name", "category_name",
        "quantity", "unit_price", "line_total",
        "total_amount", "tax_amount", "payment_amount",
        "status",
        "store_name",
        "customer_sk",
        "nps_score",
    ]

    # Es posible que algunas filas no tengan ítems (NaN). Aseguramos las columnas.
    for c in final_cols:
        if c not in obt.columns:
            obt[c] = pd.NA

    return obt[final_cols]




