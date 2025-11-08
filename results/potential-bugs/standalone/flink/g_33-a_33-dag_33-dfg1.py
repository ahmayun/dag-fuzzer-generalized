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

# Hardcoded data with all columns
data = [
    (1, "AAAAAAAABAAAAAAA", "Jimmy Allen", "3rd", -5.00),
    (2, "AAAAAAAACAAAAAAA", "Jimmy Bullock", "Cedar Spruce", -5.00),
    (3, "AAAAAAAACAAAAAAA", "Floyd Christian", "8th", -5.00),
    (4, "AAAAAAAAEAAAAAAA", "James Lachance", "6th", -5.00),
    (5, "AAAAAAAAEAAAAAAA", "James Lachance", "9th 12th", -5.00),
    (6, "AAAAAAAAEAAAAAAA", "Joaquin Washington", "Adams", -5.00),
    (7, "AAAAAAAAHAAAAAAA", "Michael Burton", "3rd", -5.00)
]

schema = [
    "s_store_sk",
    "s_store_id",
    "s_manager",
    "s_street_name",
    "s_gmt_offset"
]

# Create table from the hardcoded data
store_table = table_env.from_elements(data, schema)
table_env.create_temporary_view("store", store_table)

# Execute the pipeline
autonode_7 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_6 = autonode_7.order_by(col('s_manager_node_7'))
autonode_5 = autonode_6.group_by(col('s_street_name_node_7')).select(col('s_gmt_offset_node_7').count.alias('s_gmt_offset_node_7'))
autonode_4 = autonode_5.select(col('s_gmt_offset_node_7'))
autonode_3 = autonode_4.group_by(col('s_gmt_offset_node_7')).select(col('s_gmt_offset_node_7').avg.alias('s_gmt_offset_node_7'))
autonode_2 = autonode_3.distinct()
autonode_1 = autonode_2.alias('GiYBw')
sink = autonode_1.alias('annpZ')

print("=== Query Execution Plan ===")
print(sink.explain())

print("\n=== Query Results ===")
result = sink.to_pandas()
print(result)