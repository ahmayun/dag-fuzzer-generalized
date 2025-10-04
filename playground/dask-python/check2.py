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

web_returns = dd.read_csv('tpcds-csv/web_returns/*.csv')
store_returns = dd.read_csv('tpcds-csv/store_returns/*.csv')
web_page = dd.read_csv('tpcds-csv/web_page/*.csv')
customer = dd.read_csv('tpcds-csv/customer/*.csv')
catalog_sales = dd.read_csv('tpcds-csv/catalog_sales/*.csv')
household_demographics = dd.read_csv('tpcds-csv/household_demographics/*.csv')
web_sales = dd.read_csv('tpcds-csv/web_sales/*.csv')
warehouse = dd.read_csv('tpcds-csv/warehouse/*.csv')

autonode_25 = household_demographics.rename(columns=lambda col: f'{{col}}_node_25')
autonode_26 = customer.rename(columns=lambda col: f'{{col}}_node_26')
autonode_24 = store_returns.rename(columns=lambda col: f'{{col}}_node_24')
autonode_28 = web_returns.rename(columns=lambda col: f'{{col}}_node_28')
autonode_21 = web_page.rename(columns=lambda col: f'{{col}}_node_21')
autonode_27 = catalog_sales.rename(columns=lambda col: f'{{col}}_node_27')
autonode_23 = store_returns.rename(columns=lambda col: f'{{col}}_node_23')
autonode_22 = household_demographics.rename(columns=lambda col: f'{{col}}_node_22')
autonode_29 = warehouse.rename(columns=lambda col: f'{{col}}_node_29')
autonode_17 = autonode_25.merge(autonode_26, 'c_customer_sk_node_26')
autonode_16 = autonode_24.drop_duplicates(['sr_return_amt_node_24'])
autonode_19 = autonode_28.map_partitions(lambda x: x)
autonode_13 = autonode_21.dropna(['wp_web_page_id_node_21'])
autonode_18 = autonode_27.drop_duplicates('cs_net_profit_node_27')
autonode_15 = autonode_23.repartition(73)
autonode_14 = autonode_22.repartition(6)
autonode_20 = autonode_29.assign({'y3UrTxEv': 'w_gmt_offset_node_29'})
autonode_11 = autonode_19.set_index('o18EQ')
autonode_7 = autonode_13.drop_duplicates(['wp_max_ad_count_node_21'])
autonode_10 = autonode_17.merge(autonode_18)
autonode_9 = autonode_15.merge(autonode_16, 'sr_store_sk_node_23', ['sr_return_tax_node_23'])
autonode_8 = autonode_14.query('hd_income_band_sk_node_22')
autonode_12 = autonode_20.rename({'mozY5': 'WBue8'})
autonode_3 = autonode_7.rename({'WGffd': 'wzHOa'})
autonode_5 = autonode_10.merge(autonode_11, ['c_first_sales_date_sk_node_26'], 'c_current_cdemo_sk_node_26')
autonode_4 = autonode_8.merge(autonode_9, ['sr_net_loss_node_23'], 'sr_item_sk_node_23', ['sr_return_ship_cost_node_24'])
autonode_6 = autonode_12.sort_values(['w_suite_number_node_29'])
autonode_1 = autonode_3.merge(autonode_4, ['sr_net_loss_node_23'], 'sr_return_quantity_node_23')
autonode_2 = autonode_5.merge(autonode_6, 'cs_call_center_sk_node_27', ['c_first_name_node_26'])
sink = autonode_1.merge(autonode_2, ['sr_item_sk_node_23'], ['c_preferred_cust_flag_node_26'])
print(sink.head(10))