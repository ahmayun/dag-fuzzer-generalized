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


def preloaded_aggregation(values: pd.Series) -> int:
    return values.count()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_24 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_24") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_18 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_18") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_19 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_19") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_21 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_21") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_20 = table_env.from_path("reason").select(*[col(column_name).alias(f"{column_name}_node_20") for column_name in table_env.from_path("reason").get_schema().get_field_names()])
autonode_23 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_23") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_22 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_22") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_12 = autonode_18.limit(49)
autonode_13 = autonode_19.distinct()
autonode_15 = autonode_21.order_by(col('ca_city_node_21'))
autonode_14 = autonode_20.select(col('r_reason_id_node_20'))
autonode_17 = autonode_23.join(autonode_24, col('cp_end_date_sk_node_24') == col('ib_upper_bound_node_23'))
autonode_16 = autonode_22.distinct()
autonode_7 = autonode_12.add_columns(lit("hello"))
autonode_9 = autonode_15.order_by(col('ca_address_sk_node_21'))
autonode_8 = autonode_13.join(autonode_14, col('r_reason_id_node_20') == col('ca_street_name_node_19'))
autonode_11 = autonode_17.distinct()
autonode_10 = autonode_16.select(col('inv_warehouse_sk_node_22'))
autonode_4 = autonode_7.order_by(col('i_rec_end_date_node_18'))
autonode_5 = autonode_8.join(autonode_9, col('ca_gmt_offset_node_21') == col('ca_gmt_offset_node_19'))
autonode_6 = autonode_10.join(autonode_11, col('inv_warehouse_sk_node_22') == col('ib_lower_bound_node_23'))
autonode_2 = autonode_4.join(autonode_5, col('ca_address_sk_node_21') == col('i_manufact_id_node_18'))
autonode_3 = autonode_6.filter(col('cp_type_node_24').char_length > 5)
autonode_1 = autonode_2.join(autonode_3, col('ca_address_sk_node_21') == col('ib_upper_bound_node_23'))
sink = autonode_1.group_by(col('ca_gmt_offset_node_19')).select(col('cp_catalog_page_id_node_24').min.alias('cp_catalog_page_id_node_24'))
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