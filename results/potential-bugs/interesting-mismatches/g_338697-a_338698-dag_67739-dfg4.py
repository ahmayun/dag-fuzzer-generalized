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

autonode_13 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_14 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_15 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_16 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_10 = autonode_13.order_by(col('cp_catalog_page_id_node_13'))
autonode_11 = autonode_14.join(autonode_15, col('p_promo_sk_node_14') == col('cd_demo_sk_node_15'))
autonode_12 = autonode_16.distinct()
autonode_7 = autonode_10.order_by(col('cp_type_node_13'))
autonode_8 = autonode_11.add_columns(lit("hello"))
autonode_9 = autonode_12.distinct()
autonode_5 = autonode_7.join(autonode_8, col('p_discount_active_node_14') == col('cp_type_node_13'))
autonode_6 = autonode_9.add_columns(lit("hello"))
autonode_3 = autonode_5.group_by(col('cp_catalog_page_number_node_13')).select(col('p_channel_catalog_node_14').min.alias('p_channel_catalog_node_14'))
autonode_4 = autonode_6.order_by(col('cp_end_date_sk_node_16'))
autonode_1 = autonode_3.filter(col('p_channel_catalog_node_14').char_length >= 5)
autonode_2 = autonode_4.alias('cCnCQ')
sink = autonode_1.join(autonode_2, col('cp_description_node_16') == col('p_channel_catalog_node_14'))
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