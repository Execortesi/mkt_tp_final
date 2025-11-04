import pandas as pd
from datetime import datetime, timedelta

def _collect_datetimes(raw: dict[str, pd.DataFrame], table: str, cols: list[str]) -> list[pd.Series]:
    if table not in raw:
        return []
    df = raw[table]
    out = []
    for c in cols:
        if c in df.columns:
            out.append(pd.to_datetime(df[c], errors="coerce"))
    return out

def transform_dim_calendar(raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Recolectamos todas las columnas de fechas relevantes (si existen)
    candidates: list[pd.Series] = []
    candidates += _collect_datetimes(raw_data, "sales_order",   ["order_date"])
    candidates += _collect_datetimes(raw_data, "shipment",      ["shipped_at", "delivered_at"])
    candidates += _collect_datetimes(raw_data, "web_session",   ["started_at", "ended_at"])
    candidates += _collect_datetimes(raw_data, "nps_response",  ["responded_at"])
    candidates += _collect_datetimes(raw_data, "customer",      ["created_at"])
    candidates += _collect_datetimes(raw_data, "address",       ["created_at"])
    candidates += _collect_datetimes(raw_data, "product",       ["created_at"])
    # (agregá más si tu dataset crudo tiene otras fechas)

    if candidates:
        s = pd.concat(candidates).dropna()
        if not s.empty:
            min_d = s.min().normalize()
            max_d = s.max().normalize()
        else:
            min_d = (pd.Timestamp.today() - pd.Timedelta(days=365)).normalize()
            max_d = (pd.Timestamp.today() + pd.Timedelta(days=365)).normalize()
    else:
        # Fallback por si no hay fechas en crudo
        min_d = (pd.Timestamp.today() - pd.Timedelta(days=365)).normalize()
        max_d = (pd.Timestamp.today() + pd.Timedelta(days=365)).normalize()

    # Garantizamos al menos un día
    if min_d > max_d:
        min_d, max_d = max_d, min_d

    dates = pd.date_range(start=min_d, end=max_d, freq="D")
    cal = pd.DataFrame({"date": dates})

    # Campos solicitados en el PDF
    cal["date_sk"] = (cal["date"].dt.year * 10000 + cal["date"].dt.month * 100 + cal["date"].dt.day).astype(int)
    cal["day"] = cal["date"].dt.day.astype(int)
    cal["month"] = cal["date"].dt.month.astype(int)
    cal["year"] = cal["date"].dt.year.astype(int)
    cal["day_name"] = cal["date"].dt.day_name()        # (queda en inglés salvo que tengas locale en español)
    cal["month_name"] = cal["date"].dt.month_name()
    cal["quarter"] = cal["date"].dt.quarter.astype(int)
    cal["week_number"] = cal["date"].dt.isocalendar().week.astype(int)
    cal["year_month"] = cal["date"].dt.strftime("%Y-%m")
    cal["is_weekend"] = cal["date"].dt.dayofweek >= 5   # bool

    # Orden final
    cols = [
        "date_sk", "date", "day", "month", "year",
        "day_name", "month_name", "quarter", "week_number",
        "year_month", "is_weekend"
    ]
    # Surrogate key opcional: usamos date_sk como PK, no agrego otra SK
    return cal[cols].sort_values("date_sk").reset_index(drop=True)

