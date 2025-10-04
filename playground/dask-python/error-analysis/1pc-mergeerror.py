import dask.dataframe as dd
import pandas as pd
import numpy as np

import pandas as pd
import numpy as np

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

autonode_10 = date_dim.rename(columns=lambda col: f'{col}_node_10')
autonode_12 = warehouse.rename(columns=lambda col: f'{col}_node_12')
autonode_11 = web_sales.rename(columns=lambda col: f'{col}_node_11')
autonode_7 = autonode_10.head(n=72, compute=True)
autonode_9 = autonode_12.dropna(subset=['w_zip_node_12'], how='any')
autonode_8 = autonode_11.compute()
autonode_6 = autonode_9.dropna(subset=['w_street_type_node_12'], how='all')
autonode_5 = autonode_7.merge(autonode_8, on='ws_ext_ship_cost_node_11', left_on='d_dom_node_10', right_on=['d_day_name_node_10'], how='outer')
autonode_4 = autonode_6.drop_duplicates(subset=['w_warehouse_name_node_12'])
autonode_3 = autonode_5.loc(indexer='ws_net_paid_inc_tax_node_11 < 12.2894287109375')
autonode_2 = autonode_4.sort_values(by=['w_street_number_node_12'], ascending='m6cmyc5k')
autonode_1 = autonode_3.loc(indexer='d_weekend_node_10.str.len() > 5')
sink = autonode_1.merge(autonode_2, on=['ws_net_profit_node_11'], left_on='d_weekend_node_10', right_on=['w_country_node_12'], how='left')
print(sink.head(10))