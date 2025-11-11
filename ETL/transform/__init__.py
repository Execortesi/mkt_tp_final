import pandas as pd

# DIMS
from .build_dim_channel import build_dim_channel
from .build_dim_customer import transform_dim_customer
from .build_dim_product import transform_dim_product
from .build_dim_store import transform_dim_store
from .build_dim_address import build_dim_address
from .build_dim_calendar import transform_dim_calendar


# FACTS
from .build_fact_sales_order import transform_fact_sales_order
from .build_fact_sales_order_item import transform_fact_sales_order_item
from .build_fact_payment import transform_fact_payment
from .build_fact_shipment import transform_fact_shipment
from .build_fact_web_session import transform_fact_web_session
from .build_fact_nps_response import transform_fact_nps_response

def transform_dimensions(raw_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    return {
        "dim_channel": build_dim_channel(raw_data),
        "dim_customer": transform_dim_customer(raw_data),
        "dim_product": transform_dim_product(raw_data),
        "dim_store": transform_dim_store(raw_data),
        "dim_address": build_dim_address(raw_data),      
        "dim_calendar": transform_dim_calendar(raw_data), 
    }

def transform_facts(raw_data: dict[str, pd.DataFrame], dims: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    return {
        "fact_sales_order": transform_fact_sales_order(raw_data, dims),
        "fact_sales_order_item": transform_fact_sales_order_item(raw_data, dims),
        "fact_payment": transform_fact_payment(raw_data, dims),
        "fact_shipment": transform_fact_shipment(raw_data, dims),
        "fact_web_session": transform_fact_web_session(raw_data, dims),
        "fact_nps_response": transform_fact_nps_response(raw_data, dims),
    }




