# ======== Program ========
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

# Load tables from CSV
date_dim = dd.read_csv('tpcds-csv/date_dim/*.csv')
customer = dd.read_csv('tpcds-csv/customer/*.csv')
household_demographics = dd.read_csv('tpcds-csv/household_demographics/*.csv')
web_sales = dd.read_csv('tpcds-csv/web_sales/*.csv')

autonode_10 = date_dim.rename(columns=lambda col: f'{col}_node_10')
autonode_13 = customer.rename(columns=lambda col: f'{col}_node_13')
autonode_12 = household_demographics.rename(columns=lambda col: f'{col}_node_12')
autonode_11 = web_sales.rename(columns=lambda col: f'{col}_node_11')
autonode_14 = date_dim.rename(columns=lambda col: f'{col}_node_14')
autonode_6 = autonode_10.assign({'lQe66rHY': 'd_dow_node_10'})
autonode_8 = autonode_12.rename({'PaLUn': 'ecFz5'})
autonode_7 = autonode_11.groupby(['ws_bill_addr_sk_node_11']).agg({'ws_promo_sk_node_11': 'mean'}).reset_index()
autonode_9 = autonode_13.merge(autonode_14)
autonode_4 = autonode_6.merge(autonode_7, ['d_fy_week_seq_node_10'], ['d_fy_year_node_10'], 'd_quarter_name_node_10')
autonode_5 = autonode_8.merge(autonode_9, ['c_first_sales_date_sk_node_13'], 'c_salutation_node_13', ['c_birth_country_node_13'])
autonode_3 = autonode_4.merge(autonode_5, 'd_same_day_ly_node_10', 'd_fy_quarter_seq_node_14', ['d_dom_node_10'])
autonode_2 = autonode_3.loc('x[['d_day_name_node_14']].apply(lambda row: preloaded_udf_complex(*row), axis=1)')
autonode_1 = autonode_2.repartition(15)
sink = autonode_1.head()
print(sink.head(10))