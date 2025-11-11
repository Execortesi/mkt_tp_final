import pandas as pd

# Calendario desde el rango de fechas real de ventas
def transform_dim_calendar(raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    so = raw_data.get("sales_order", pd.DataFrame()).copy()

    # Detectamos rango de fechas; fallback si viniera vacío
    if "order_date" in so.columns and not so.empty:
        so["order_date"] = pd.to_datetime(so["order_date"], errors="coerce")
        start = so["order_date"].min()
        end = so["order_date"].max()
    else:
        # fallback seguro
        start = pd.Timestamp("2021-01-01")
        end = pd.Timestamp("2021-12-31")

    # Normalizamos (por si vienen timestamps)
    start = pd.to_datetime(start).normalize()
    end = pd.to_datetime(end).normalize()

    dates = pd.date_range(start=start, end=end, freq="D")
    cal = pd.DataFrame({"date": dates})

    # Clave surrogate estilo AAAAMMDD
    cal["date_sk"] = cal["date"].dt.strftime("%Y%m%d").astype(int)

    cal["day"] = cal["date"].dt.day
    cal["month"] = cal["date"].dt.month
    cal["year"] = cal["date"].dt.year
    cal["quarter"] = cal["date"].dt.quarter
    cal["week_number"] = cal["date"].dt.isocalendar().week.astype(int)
    cal["is_weekend"] = cal["date"].dt.dayofweek >= 5

    meses_es = [
        "Enero","Febrero","Marzo","Abril","Mayo","Junio",
        "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
    ]
    cal["month_name"] = cal["month"].map(lambda m: meses_es[m-1])

    # Para filtros y series
    cal["year_month"] = cal["date"].dt.strftime("%Y-%m")
    cal["month_date"] = cal["date"].values.astype("datetime64[M]")  # primer día del mes

    cols = [
        "date_sk","date","day","month","year","month_name",
        "quarter","week_number","is_weekend","year_month","month_date"
    ]
    return cal[cols].sort_values("date_sk").reset_index(drop=True)



