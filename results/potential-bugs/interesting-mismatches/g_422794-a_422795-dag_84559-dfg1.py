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
    import numpy as np
    return np.exp(np.log(values[values > 0]).mean()) if (values > 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_12 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_11 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_14 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_9 = autonode_12.distinct()
autonode_8 = autonode_11.select(col('w_street_type_node_11'))
autonode_10 = autonode_13.join(autonode_14, col('ws_ship_cdemo_sk_node_14') == col('sm_ship_mode_sk_node_13'))
autonode_6 = autonode_8.join(autonode_9, col('d_current_week_node_12') == col('w_street_type_node_11'))
autonode_7 = autonode_10.alias('wwHkx')
autonode_4 = autonode_6.order_by(col('d_current_quarter_node_12'))
autonode_5 = autonode_7.filter(col('ws_coupon_amt_node_14') > -5.780655145645142)
autonode_3 = autonode_4.join(autonode_5, col('w_street_type_node_11') == col('sm_contract_node_13'))
autonode_2 = autonode_3.group_by(col('d_week_seq_node_12')).select(col('ws_net_paid_inc_tax_node_14').min.alias('ws_net_paid_inc_tax_node_14'))
autonode_1 = autonode_2.add_columns(lit("hello"))
sink = autonode_1.order_by(col('ws_net_paid_inc_tax_node_14'))
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