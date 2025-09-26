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

autonode_13 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_12 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_14 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_15 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_16 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_8 = autonode_12.join(autonode_13, col('cd_education_status_node_12') == col('s_manager_node_13'))
autonode_9 = autonode_14.order_by(col('i_rec_end_date_node_14'))
autonode_10 = autonode_15.filter(col('i_brand_id_node_15') < -7)
autonode_11 = autonode_16.filter(col('web_tax_percentage_node_16') < -10.665464401245117)
autonode_6 = autonode_8.join(autonode_9, col('i_category_id_node_14') == col('s_store_sk_node_13'))
autonode_7 = autonode_10.join(autonode_11, col('web_gmt_offset_node_16') == col('i_wholesale_cost_node_15'))
autonode_4 = autonode_6.order_by(col('i_item_id_node_14'))
autonode_5 = autonode_7.distinct()
autonode_3 = autonode_4.join(autonode_5, col('cd_dep_employed_count_node_12') == col('web_mkt_id_node_16'))
autonode_2 = autonode_3.group_by(col('i_item_desc_node_14')).select(col('i_manager_id_node_14').min.alias('i_manager_id_node_14'))
autonode_1 = autonode_2.limit(50)
sink = autonode_1.group_by(col('i_manager_id_node_14')).select(col('i_manager_id_node_14').avg.alias('i_manager_id_node_14'))
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