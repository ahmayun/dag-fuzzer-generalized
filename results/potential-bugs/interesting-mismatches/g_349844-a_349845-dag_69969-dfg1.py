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
    return values.max()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_9 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_8 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_11 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_6 = autonode_10.order_by(col('d_last_dom_node_10'))
autonode_5 = autonode_8.join(autonode_9, col('sr_reason_sk_node_9') == col('hd_demo_sk_node_8'))
autonode_7 = autonode_11.filter(col('ws_promo_sk_node_11') < 1)
autonode_3 = autonode_5.add_columns(lit("hello"))
autonode_4 = autonode_6.join(autonode_7, col('ws_warehouse_sk_node_11') == col('d_month_seq_node_10'))
autonode_2 = autonode_3.join(autonode_4, col('d_current_quarter_node_10') == col('hd_buy_potential_node_8'))
autonode_1 = autonode_2.group_by(col('d_fy_year_node_10')).select(col('d_month_seq_node_10').max.alias('d_month_seq_node_10'))
sink = autonode_1.group_by(col('d_month_seq_node_10')).select(col('d_month_seq_node_10').max.alias('d_month_seq_node_10'))
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