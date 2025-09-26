# ======== Program ========
from pyflink.table import *
from pyflink.table.expressions import *
from pyflink.table.udf import udf
from pyflink.table.types import DataTypes

from pyflink.table.udf import AggregateFunction, udaf
from pyflink.table import DataTypes
import pandas as pd

class MyObject:
    def __init__(self, name, value):
        self.name = name
        self.value = value

# UDF that returns the custom object
@udf(result_type=DataTypes.ROW([
    DataTypes.FIELD("name", DataTypes.STRING()),
    DataTypes.FIELD("value", DataTypes.INT())
]))
def preloaded_udf_complex(*input_val):
    obj = MyObject("test", hash(input_val[0]))
    return (obj.name, obj.value)  # Return as tuple

@udf(result_type=DataTypes.BOOLEAN())
def preloaded_udf_boolean(input_val):
    return True


def preloaded_aggregation(values: pd.Series) -> float:
    return values.iloc[0] if len(values) > 0 else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_12 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_14 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_15 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_9 = autonode_13.group_by(col('cs_bill_hdemo_sk_node_13')).select(col('cs_sold_date_sk_node_13').count.alias('cs_sold_date_sk_node_13'))
autonode_8 = autonode_12.alias('V8ZbK')
autonode_10 = autonode_14.add_columns(lit("hello"))
autonode_11 = autonode_15.limit(83)
autonode_5 = autonode_8.join(autonode_9, col('p_item_sk_node_12') == col('cs_sold_date_sk_node_13'))
autonode_6 = autonode_10.order_by(col('cs_bill_addr_sk_node_14'))
autonode_7 = autonode_11.alias('xNFEh')
autonode_3 = autonode_5.order_by(col('p_channel_details_node_12'))
autonode_4 = autonode_6.join(autonode_7, col('cs_ship_mode_sk_node_15') == col('cs_ship_hdemo_sk_node_14'))
autonode_2 = autonode_3.join(autonode_4, col('p_cost_node_12') == col('cs_ext_wholesale_cost_node_14'))
autonode_1 = autonode_2.filter(col('cs_ship_hdemo_sk_node_14') <= 15)
sink = autonode_1.alias('q67qV')
print(sink.explain())

# ======== Details ========
"""
{
  "is_same": false,
  "result_name": "MismatchException",
  "result_details": {
    "opt_error": "TableException",
    "unopt_error": "RuntimeException"
  }
}
"""



//Optimizer Branch Coverage: 0