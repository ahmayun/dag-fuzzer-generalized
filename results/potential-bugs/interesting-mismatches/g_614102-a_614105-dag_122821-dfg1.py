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
    return len(values) / (1.0 / values).sum() if (values != 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_9 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_8 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_7 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_6 = autonode_9.select(col('i_item_id_node_9'))
autonode_5 = autonode_8.order_by(col('sm_carrier_node_8'))
autonode_4 = autonode_7.order_by(col('p_channel_tv_node_7'))
autonode_3 = autonode_5.join(autonode_6, col('sm_code_node_8') == col('i_item_id_node_9'))
autonode_2 = autonode_4.order_by(col('p_channel_email_node_7'))
autonode_1 = autonode_2.join(autonode_3, col('sm_ship_mode_sk_node_8') == col('p_promo_sk_node_7'))
sink = autonode_1.group_by(col('sm_carrier_node_8')).select(col('sm_contract_node_8').min.alias('sm_contract_node_8'))
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