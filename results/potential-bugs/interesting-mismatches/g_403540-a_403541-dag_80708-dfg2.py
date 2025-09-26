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

autonode_13 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_17 = table_env.from_path("reason").select(*[col(column_name).alias(f"{column_name}_node_17") for column_name in table_env.from_path("reason").get_schema().get_field_names()])
autonode_14 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_15 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_16 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_8 = autonode_13.order_by(col('d_holiday_node_13'))
autonode_12 = autonode_17.order_by(col('r_reason_desc_node_17'))
autonode_9 = autonode_14.alias('wAbCY')
autonode_10 = autonode_15.alias('mwKHI')
autonode_11 = autonode_16.order_by(col('sm_type_node_16'))
autonode_7 = autonode_12.order_by(col('r_reason_id_node_17'))
autonode_4 = autonode_8.join(autonode_9, col('hd_buy_potential_node_14') == col('d_holiday_node_13'))
autonode_5 = autonode_10.alias('GXUVc')
autonode_6 = autonode_11.alias('RqPmK')
autonode_2 = autonode_4.join(autonode_5, col('d_moy_node_13') == col('inv_item_sk_node_15'))
autonode_3 = autonode_6.join(autonode_7, col('sm_carrier_node_16') == col('r_reason_id_node_17'))
autonode_1 = autonode_2.join(autonode_3, col('d_same_day_lq_node_13') == col('r_reason_sk_node_17'))
sink = autonode_1.group_by(col('r_reason_desc_node_17')).select(col('d_dom_node_13').max.alias('d_dom_node_13'))
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