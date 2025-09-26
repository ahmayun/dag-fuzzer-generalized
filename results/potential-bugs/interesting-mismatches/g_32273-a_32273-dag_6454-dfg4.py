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


def preloaded_aggregation(values: pd.Series) -> int:
    return values.nunique()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_13 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_12 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_11 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_7 = autonode_10.limit(91)
autonode_9 = autonode_12.join(autonode_13, col('p_cost_node_12') == col('p_cost_node_13'))
autonode_8 = autonode_11.select(col('i_item_desc_node_11'))
autonode_4 = autonode_7.alias('93SAp')
autonode_6 = autonode_9.order_by(col('p_promo_id_node_13'))
autonode_5 = autonode_8.add_columns(lit("hello"))
autonode_2 = autonode_4.distinct()
autonode_3 = autonode_5.join(autonode_6, col('i_item_desc_node_11') == col('p_channel_event_node_13'))
autonode_1 = autonode_2.join(autonode_3, col('ws_web_site_sk_node_10') == col('p_promo_sk_node_13'))
sink = autonode_1.group_by(col('p_channel_details_node_13')).select(col('p_promo_sk_node_12').min.alias('p_promo_sk_node_12'))
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