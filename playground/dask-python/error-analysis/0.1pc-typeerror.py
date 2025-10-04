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

autonode_6 = income_band.rename(columns=lambda col: f'{col}_node_6')
autonode_8 = item.rename(columns=lambda col: f'{col}_node_8')
autonode_7 = ship_mode.rename(columns=lambda col: f'{col}_node_7')
autonode_4 = autonode_6.head(n=27, compute=True)
autonode_5 = autonode_7.merge(autonode_8, on='i_category_node_8', left_on='sm_code_node_7', right_on='i_item_desc_node_8', how='left')
autonode_3 = autonode_4.merge(autonode_5, on=['i_item_desc_node_8'], left_on='ib_lower_bound_node_6', right_on=['sm_code_node_7'], how='right')
autonode_2 = autonode_3.sort_values(by='i_manufact_id_node_8', ascending='8AKtZNZN')
autonode_1 = autonode_2.head(n=94, compute=False)
sink = autonode_1.compute()
print(sink.head(10))