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
    return values.median()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_7 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_4 = autonode_6.order_by(col('cp_catalog_number_node_6'))
autonode_5 = autonode_7.alias('viVcJ')
autonode_3 = autonode_4.join(autonode_5, col('cp_description_node_6') == col('i_color_node_7'))
autonode_2 = autonode_3.group_by(col('cp_end_date_sk_node_6')).select(col('cp_catalog_page_sk_node_6').max.alias('cp_catalog_page_sk_node_6'))
autonode_1 = autonode_2.select(col('cp_catalog_page_sk_node_6'))
sink = autonode_1.select(col('cp_catalog_page_sk_node_6'))
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