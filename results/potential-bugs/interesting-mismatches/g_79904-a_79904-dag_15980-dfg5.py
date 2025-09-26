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
    return values.mean()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_4 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_4") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_6 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_5 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_2 = autonode_4.order_by(col('ws_bill_customer_sk_node_4'))
autonode_3 = autonode_5.join(autonode_6, col('sm_ship_mode_sk_node_6') == col('inv_quantity_on_hand_node_5'))
autonode_1 = autonode_2.join(autonode_3, col('ws_bill_cdemo_sk_node_4') == col('inv_date_sk_node_5'))
sink = autonode_1.group_by(col('ws_promo_sk_node_4')).select(col('ws_web_page_sk_node_4').max.alias('ws_web_page_sk_node_4'))
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