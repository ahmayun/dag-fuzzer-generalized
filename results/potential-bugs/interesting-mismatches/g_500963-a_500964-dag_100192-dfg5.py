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
    return values.kurtosis()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_8 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_7 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_6 = autonode_8.limit(60)
autonode_5 = autonode_7.group_by(col('cp_end_date_sk_node_7')).select(col('cp_catalog_number_node_7').avg.alias('cp_catalog_number_node_7'))
autonode_4 = autonode_6.order_by(col('ws_web_page_sk_node_8'))
autonode_3 = autonode_5.add_columns(lit("hello"))
autonode_2 = autonode_3.join(autonode_4, col('ws_bill_cdemo_sk_node_8') == col('cp_catalog_number_node_7'))
autonode_1 = autonode_2.group_by(col('ws_net_paid_inc_ship_tax_node_8')).select(col('ws_promo_sk_node_8').min.alias('ws_promo_sk_node_8'))
sink = autonode_1.distinct()
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