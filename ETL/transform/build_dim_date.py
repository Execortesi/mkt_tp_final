import pandas as pd

def transform_dim_date(raw_data):
    so = raw_data["sales_order"].copy()
    so["order_date"] = pd.to_datetime(so["order_date"], errors="coerce")

    dates = pd.DataFrame({"date": so["order_date"].dropna().drop_duplicates()}).sort_values("date")
    dates = dates.reset_index(drop=True)
    dates.insert(0, "date_sk", range(1, 1 + len(dates)))

    dates["year"]  = dates["date"].dt.year
    dates["month"] = dates["date"].dt.month
    dates["day"]   = dates["date"].dt.day
    dates["dow"]   = dates["date"].dt.weekday + 1
    dates["week"]  = dates["date"].dt.isocalendar().week.astype(int)
    dates["ym"]    = dates["date"].dt.strftime("%Y-%m")

    return dates[["date_sk", "date", "year", "month", "day", "week", "dow", "ym"]]
