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
    return values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_9 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_12 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_11 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_6 = autonode_10.select(col('t_am_pm_node_10'))
autonode_5 = autonode_9.order_by(col('cp_type_node_9'))
autonode_8 = autonode_12.limit(27)
autonode_7 = autonode_11.order_by(col('i_container_node_11'))
autonode_3 = autonode_5.join(autonode_6, col('t_am_pm_node_10') == col('cp_department_node_9'))
autonode_4 = autonode_7.join(autonode_8, col('inv_quantity_on_hand_node_12') == col('i_class_id_node_11'))
autonode_2 = autonode_3.join(autonode_4, col('cp_catalog_page_id_node_9') == col('i_container_node_11'))
autonode_1 = autonode_2.group_by(col('i_brand_node_11')).select(col('cp_start_date_sk_node_9').min.alias('cp_start_date_sk_node_9'))
sink = autonode_1.filter(col('cp_start_date_sk_node_9') <= -14)
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