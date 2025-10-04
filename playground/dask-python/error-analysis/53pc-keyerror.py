import dask.dataframe as dd
import pandas as pd
import numpy as np

import pandas as pd
import numpy as np
from utils import load_dask_table

# String Filter UDFs
def filter_udf_string(arg):
    return len(str(arg)) > 5

# Integer Filter UDFs
def filter_udf_integer(arg):
    return arg > 0 and arg % 2 == 0

# Decimal Filter UDFs
def filter_udf_decimal(arg):
    return arg > 0.0 and arg < 1000.0

# Complex UDF
def preloaded_udf_complex(*args):
    return hash(args[0]) if len(args) > 0 else 0



customer_address = load_dask_table('customer_address')
web_site = load_dask_table('web_site')
item = load_dask_table('item')
catalog_returns = load_dask_table('catalog_returns')
catalog_sales = load_dask_table('catalog_sales')


autonode_13 = customer_address.rename(columns=lambda col: f'{col}_node_13')
autonode_12 = web_site.rename(columns=lambda col: f'{col}_node_12')
autonode_11 = item.rename(columns=lambda col: f'{col}_node_11')
autonode_14 = catalog_returns.rename(columns=lambda col: f'{col}_node_14')
autonode_15 = catalog_sales.rename(columns=lambda col: f'{col}_node_15')
autonode_8 = autonode_12.map_partitions(func=lambda x: x, meta='r3XUK5LO')
autonode_7 = autonode_11.compute()
autonode_9 = autonode_13.merge(autonode_14, on='cr_returned_date_sk_node_14', left_on=['cr_return_quantity_node_14'], right_on=['cr_refunded_addr_sk_node_14'], how='right')
autonode_10 = autonode_15.query(expr='cs_ship_cdemo_sk_node_15')
autonode_5 = autonode_7.merge(autonode_8, on=['i_wholesale_cost_node_11'], left_on=['web_name_node_12'], right_on='i_size_node_11', how='right')
autonode_6 = autonode_9.merge(autonode_10, on='cs_bill_cdemo_sk_node_15', left_on='cr_returning_addr_sk_node_14', right_on='cs_list_price_node_15', how='inner')
autonode_4 = autonode_5.merge(autonode_6, on=['cr_item_sk_node_14'], left_on='web_site_id_node_12', right_on=['i_product_name_node_11'], how='inner')
autonode_3 = autonode_4.drop_duplicates(subset='cs_catalog_page_sk_node_15')
autonode_2 = autonode_3.loc(indexer='web_site_id_node_12.str.len() > 5')
autonode_1 = autonode_2.loc(indexer='web_class_node_12.str.len() >= 5')
sink = autonode_1.fillna(value={'web_open_date_sk_node_12': 49})
print(sink.head(10))