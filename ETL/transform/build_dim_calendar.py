import pandas as pd

def _from_series(s: pd.Series) -> pd.DataFrame:
    s = pd.to_datetime(s.dropna().unique())
    df = pd.DataFrame({"date": s})

    # Clave y desgloses
    df["date_sk"] = df["date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["date"].dt.year
    df["quarter"] = df["date"].dt.quarter
    df["month"] = df["date"].dt.month
    df["month_name"] = df["date"].dt.month_name(locale="es_ES") if hasattr(df["date"].dt, "month_name") else df["date"].dt.month_name()
    df["day"] = df["date"].dt.day
    df["dow"] = df["date"].dt.dayofweek + 1  # 1=Lunes … 7=Domingo
    df["day_name"] = df["date"].dt.day_name(locale="es_ES") if hasattr(df["date"].dt, "day_name") else df["date"].dt.day_name()
    # semana ISO (1–53)
    try:
        df["week_number"] = df["date"].dt.isocalendar().week.astype(int)
    except Exception:
        df["week_number"] = df["date"].dt.isocalendar().week
    # fin de semana
    df["is_weekend"] = df["dow"].isin([6, 7])
    # YYYY-MM útil para Looker Studio
    df["year_month"] = df["date"].dt.strftime("%Y-%m")

    # Orden y salida
    cols = [
        "date_sk", "date",
        "year", "quarter", "month", "month_name",
        "day", "dow", "day_name",
        "week_number", "is_weekend", "year_month"
    ]
    return df[cols].sort_values("date_sk").reset_index(drop=True)


def transform_dim_calendar(raw: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Construye dim_date a partir de las fechas de sales_order['order_date'].
    Espera columna 'order_date' en raw['sales_order'].
    """
    so = raw.get("sales_order", pd.DataFrame()).copy()
    if so.empty or "order_date" not in so.columns:
        # Devuelve dim vacía pero con columnas correctas para evitar KeyError aguas arriba
        empty = pd.DataFrame(columns=[
            "date_sk","date","year","quarter","month","month_name",
            "day","dow","day_name","week_number","is_weekend","year_month"
        ])
        return empty

    return _from_series(so["order_date"])


