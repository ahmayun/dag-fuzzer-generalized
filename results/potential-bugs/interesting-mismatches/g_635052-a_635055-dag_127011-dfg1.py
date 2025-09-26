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

autonode_13 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_12 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_14 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_15 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_16 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_8 = autonode_12.join(autonode_13, col('cd_dep_college_count_node_13') == col('s_company_id_node_12'))
autonode_9 = autonode_14.group_by(col('c_salutation_node_14')).select(col('c_first_shipto_date_sk_node_14').max.alias('c_first_shipto_date_sk_node_14'))
autonode_10 = autonode_15.order_by(col('cd_demo_sk_node_15'))
autonode_11 = autonode_16.limit(20)
autonode_5 = autonode_8.order_by(col('cd_dep_college_count_node_13'))
autonode_6 = autonode_9.join(autonode_10, col('c_first_shipto_date_sk_node_14') == col('cd_purchase_estimate_node_15'))
autonode_7 = autonode_11.order_by(col('cp_type_node_16'))
autonode_3 = autonode_5.add_columns(lit("hello"))
autonode_4 = autonode_6.join(autonode_7, col('cp_catalog_page_sk_node_16') == col('c_first_shipto_date_sk_node_14'))
autonode_2 = autonode_3.join(autonode_4, col('cd_education_status_node_13') == col('cd_education_status_node_15'))
autonode_1 = autonode_2.group_by(col('cd_credit_rating_node_13')).select(col('s_market_id_node_12').min.alias('s_market_id_node_12'))
sink = autonode_1.order_by(col('s_market_id_node_12'))
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