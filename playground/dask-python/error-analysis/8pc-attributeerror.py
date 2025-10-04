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

autonode_3 = inventory.rename(columns=lambda col: f'{col}_node_3')
autonode_2 = autonode_3.drop_duplicates(subset='inv_warehouse_sk_node_3')
autonode_1 = autonode_2.map_partitions(func=lambda x: x, meta='dg9TC99Y')
sink = autonode_1.head(n=25, compute=False)
print(sink.head(10))