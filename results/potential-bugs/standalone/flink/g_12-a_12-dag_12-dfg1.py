from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import udf
from pyflink.table.types import DataTypes

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

# Initialize the table environment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# Hardcoded data for call_center table
data = [
    (1, "AAAAAAAABAAAAAAA", "NY Metro", "Large", 0.11),
    (2, "AAAAAAAACAAAAAAA", "Mid Atlantic", "Medium", 0.12),
    (3, "AAAAAAAACAAAAAAA", "New York", "Small", 0.10),
    (4, "AAAAAAAAEAAAAAAA", "Chicago", "Large", 0.11),
    (5, "AAAAAAAAEAAAAAAA", "Los Angeles", "Medium", 0.09),
    (6, "AAAAAAAAEAAAAAAA", "San Francisco", "Large", 0.11),
    (7, "AAAAAAAAHAAAAAAA", "Boston", "Small", 0.13),
    (8, "AAAAAAAAIAAAAAAA", "Dallas", "Medium", 0.10),
    (9, "AAAAAAAAJAAAAAAA", "Seattle", "Large", 0.12),
    (10, "AAAAAAAAKAAAAAAA", "Atlanta", "Small", 0.11)
]

schema = [
    "cc_call_center_sk",
    "cc_call_center_id",
    "cc_city",
    "cc_mkt_class",
    "cc_tax_percentage"
]

# Create table from the hardcoded data
call_center_table = table_env.from_elements(data, schema)
table_env.create_temporary_view("call_center", call_center_table)

# Execute the pipeline
autonode_5 = table_env.from_path("call_center")
autonode_4 = autonode_5.distinct()
autonode_3 = autonode_4.distinct()
autonode_2 = autonode_3.order_by(col('cc_city'))
autonode_1 = autonode_2.group_by(col('cc_tax_percentage')).select(col('cc_mkt_class').min.alias('cc_mkt_class'))
sink = autonode_1.order_by(col('cc_mkt_class'))
print(sink.explain())
