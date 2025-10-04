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

web_site = load_dask_table('web_site')
store_returns = load_dask_table('store_returns')
income_band = load_dask_table('income_band')
web_returns = load_dask_table('web_returns')


autonode_10 = web_site.rename(columns=lambda col: f'{col}_node_10')
autonode_9 = web_site.rename(columns=lambda col: f'{col}_node_9')
autonode_8 = store_returns.rename(columns=lambda col: f'{col}_node_8')
autonode_7 = income_band.rename(columns=lambda col: f'{col}_node_7')
autonode_11 = web_returns.rename(columns=lambda col: f'{col}_node_11')
autonode_5 = autonode_10.dropna(subset=['web_state_node_10'], how='any')
autonode_4 = autonode_9.drop_duplicates(subset='web_mkt_desc_node_9')
autonode_3 = autonode_7.merge(autonode_8, on=['ib_upper_bound_node_7'], left_on='ib_lower_bound_node_7', right_on=['sr_cdemo_sk_node_8'], how='inner')
autonode_6 = autonode_11.head(n=45, compute=False)
autonode_1 = autonode_3.merge(autonode_4, on=['web_mkt_desc_node_9'], left_on=['sr_hdemo_sk_node_8'], right_on='web_city_node_9', how='left')
autonode_2 = autonode_5.merge(autonode_6, on='wr_return_quantity_node_11', left_on='wr_account_credit_node_11', right_on=['web_tax_percentage_node_10'], how='inner')
sink = autonode_1.merge(autonode_2, on='wr_refunded_hdemo_sk_node_11', left_on=['web_mkt_id_node_9'], right_on='web_mkt_class_node_10', how='left')
print(sink.head(10))