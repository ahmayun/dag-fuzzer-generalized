from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from pyflink.common import Configuration
from pyflink.table.expressions import col

cfg = Configuration()

settings = (
   EnvironmentSettings.new_instance()
   .in_batch_mode()
   .with_configuration(cfg)
   .build()
)

table_env = TableEnvironment.create(settings)

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

# ======== The following data works just fine
data = [
    ("Jimmy Allen", "3rd", -5.00),
    ("Jimmy Bullock", "Cedar Spruce", -5.00),
    ("Floyd Christian", "8th", -5.00),
    ("James Lachance", "6th", -5.00),
    ("James Lachance", "9th 12th", -5.00),
    ("Joaquin Washington", "Adams", -5.00),
    ("Michael Burton", "3rd", -5.00)
]
schema = schema[2:]
# ========

source_table = table_env.from_elements(
   data,
   schema=schema
)

ordered = source_table.order_by(col('s_manager'))
aggregated = ordered.group_by(col('s_street_name')).select(col('s_gmt_offset').count.alias('s_gmt_offset'))
print(aggregated.explain())
