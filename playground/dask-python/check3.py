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

item = dd.read_csv('tpcds-csv/item/*.csv')
store_returns = dd.read_csv('tpcds-csv/store_returns/*.csv')
web_page = dd.read_csv('tpcds-csv/web_page/*.csv')
customer = dd.read_csv('tpcds-csv/customer/*.csv')
catalog_sales = dd.read_csv('tpcds-csv/catalog_sales/*.csv')
household_demographics = dd.read_csv('tpcds-csv/household_demographics/*.csv')
customer_demographics = dd.read_csv('tpcds-csv/customer_demographics/*.csv')
warehouse = dd.read_csv('tpcds-csv/warehouse/*.csv')
call_center = dd.read_csv('tpcds-csv/call_center/*.csv')
web_site = dd.read_csv('tpcds-csv/web_site/*.csv')

autonode_13 = web_page.rename(columns=lambda col: f'{{col}}_node_13')
autonode_12 = item.rename(columns=lambda col: f'{{col}}_node_12')
autonode_17 = customer_demographics.rename(columns=lambda col: f'{{col}}_node_17')
autonode_18 = warehouse.rename(columns=lambda col: f'{{col}}_node_18')
autonode_14 = call_center.rename(columns=lambda col: f'{{col}}_node_14')
autonode_15 = web_site.rename(columns=lambda col: f'{{col}}_node_15')
autonode_16 = catalog_sales.rename(columns=lambda col: f'{{col}}_node_16')
autonode_7 = autonode_12.assign(columns={'cD6TLDj4': 'i_color_node_12'})
autonode_11 = autonode_17.merge(autonode_18, on='cd_education_status_node_17', left_on=['w_street_name_node_18'], right_on=['cd_purchase_estimate_node_17'], how='outer')
autonode_8 = autonode_13.merge(autonode_14, on='cc_tax_percentage_node_14', left_on='wp_max_ad_count_node_13', right_on=['cc_mkt_id_node_14'], how='inner')
autonode_9 = autonode_15.head(n=74, compute=False)
autonode_10 = autonode_16.fillna(value=32)
autonode_4 = autonode_7.merge(autonode_8, on='cc_call_center_id_node_14', left_on='cc_closed_date_sk_node_14', right_on='i_manufact_node_12', how='inner')
autonode_5 = autonode_9.query(expr='web_tax_percentage_node_15')
autonode_6 = autonode_10.merge(autonode_11, on=['cs_catalog_page_sk_node_16'], left_on=['cd_education_status_node_17'], right_on='cs_net_paid_inc_ship_tax_node_16', how='outer')
autonode_2 = autonode_4.merge(autonode_5, on='web_rec_end_date_node_15', left_on=['cc_open_date_sk_node_14'], right_on='i_wholesale_cost_node_12', how='inner')
autonode_3 = autonode_6.dropna(subset=['cs_ext_discount_amt_node_16'], how='any')
autonode_1 = autonode_2.merge(autonode_3, on=['cs_ship_date_sk_node_16'], left_on=['cd_dep_count_node_17'], right_on=['cs_bill_hdemo_sk_node_16'], how='right')
sink = autonode_1.sort_values(by=['web_street_type_node_15'], ascending='UKpG6PQ4')
print(sink.head(10))