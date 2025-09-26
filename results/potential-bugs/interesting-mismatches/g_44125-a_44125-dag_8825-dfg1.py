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

autonode_19 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_19") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_21 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_21") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_20 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_20") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_23 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_23") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_22 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_22") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_15 = autonode_19.add_columns(lit("hello"))
autonode_17 = autonode_21.add_columns(lit("hello"))
autonode_16 = autonode_20.select(col('ib_upper_bound_node_20'))
autonode_18 = autonode_22.join(autonode_23, col('cc_employees_node_23') == col('hd_demo_sk_node_22'))
autonode_11 = autonode_15.select(col('ib_upper_bound_node_19'))
autonode_13 = autonode_17.limit(75)
autonode_12 = autonode_16.add_columns(lit("hello"))
autonode_14 = autonode_18.limit(2)
autonode_7 = autonode_11.group_by(col('ib_upper_bound_node_19')).select(col('ib_upper_bound_node_19').sum.alias('ib_upper_bound_node_19'))
autonode_9 = autonode_13.order_by(col('w_street_type_node_21'))
autonode_8 = autonode_12.order_by(col('ib_upper_bound_node_20'))
autonode_10 = autonode_14.select(col('cc_market_manager_node_23'))
autonode_4 = autonode_7.select(col('ib_upper_bound_node_19'))
autonode_5 = autonode_8.join(autonode_9, col('ib_upper_bound_node_20') == col('w_warehouse_sq_ft_node_21'))
autonode_6 = autonode_10.distinct()
autonode_2 = autonode_4.join(autonode_5, col('ib_upper_bound_node_20') == col('ib_upper_bound_node_19'))
autonode_3 = autonode_6.order_by(col('cc_market_manager_node_23'))
autonode_1 = autonode_2.join(autonode_3, col('cc_market_manager_node_23') == col('w_city_node_21'))
sink = autonode_1.limit(72)
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