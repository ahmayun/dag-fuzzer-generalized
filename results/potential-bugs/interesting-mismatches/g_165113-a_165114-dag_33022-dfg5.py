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

autonode_6 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_8 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_7 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_4 = autonode_6.order_by(col('i_class_id_node_6'))
autonode_5 = autonode_7.join(autonode_8, col('t_minute_node_8') == col('s_number_employees_node_7'))
autonode_3 = autonode_4.join(autonode_5, col('i_class_id_node_6') == col('s_store_sk_node_7'))
autonode_2 = autonode_3.group_by(col('i_color_node_6')).select(col('s_store_id_node_7').min.alias('s_store_id_node_7'))
autonode_1 = autonode_2.distinct()
sink = autonode_1.alias('NEuIf')
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