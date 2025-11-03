import pandas as pd

def transform_dim_customer(raw_data):
    df = raw_data["customer"].copy()
    df.insert(0, 'customer_sk', range(1, 1 + len(df)))

    dim_customer = df[['customer_sk', 'customer_id', 'email', 'first_name', 'last_name', 'phone', 'status', 'created_at']]

    return dim_customer
