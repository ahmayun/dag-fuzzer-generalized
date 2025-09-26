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
    return values.var()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_25 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_25") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_24 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_24") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_18 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_18") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_19 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_19") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_21 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_21") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_20 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_20") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_23 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_23") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_22 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_22") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_17 = autonode_25.order_by(col('c_preferred_cust_flag_node_25'))
autonode_16 = autonode_24.alias('wCvZq')
autonode_12 = autonode_18.add_columns(lit("hello"))
autonode_13 = autonode_19.join(autonode_20, col('ib_lower_bound_node_20') == col('wr_refunded_customer_sk_node_19'))
autonode_15 = autonode_23.distinct()
autonode_14 = autonode_21.join(autonode_22, col('d_first_dom_node_21') == col('cs_ship_date_sk_node_22'))
autonode_11 = autonode_16.join(autonode_17, col('ib_lower_bound_node_24') == col('c_birth_year_node_25'))
autonode_9 = autonode_12.join(autonode_13, col('wr_reversed_charge_node_18') == col('wr_account_credit_node_19'))
autonode_10 = autonode_14.join(autonode_15, col('d_same_day_lq_node_21') == col('ib_upper_bound_node_23'))
autonode_8 = autonode_11.group_by(col('c_customer_sk_node_25')).select(col('c_first_sales_date_sk_node_25').min.alias('c_first_sales_date_sk_node_25'))
autonode_7 = autonode_9.join(autonode_10, col('wr_account_credit_node_18') == col('cs_ext_wholesale_cost_node_22'))
autonode_6 = autonode_8.group_by(col('c_first_sales_date_sk_node_25')).select(call('preloaded_udf_agg', col('c_first_sales_date_sk_node_25')).alias('c_first_sales_date_sk_node_25'))
autonode_5 = autonode_7.filter(preloaded_udf_boolean(col('wr_returning_addr_sk_node_19')))
autonode_4 = autonode_6.order_by(col('c_first_sales_date_sk_node_25'))
autonode_3 = autonode_5.alias('3ruoN')
autonode_2 = autonode_4.add_columns(lit("hello"))
autonode_1 = autonode_3.select(col('wr_returning_addr_sk_node_18'))
sink = autonode_1.join(autonode_2, col('wr_returning_addr_sk_node_18') == col('c_first_sales_date_sk_node_25'))
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