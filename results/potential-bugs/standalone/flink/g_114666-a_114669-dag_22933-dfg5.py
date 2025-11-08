from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import udf
from pyflink.table.types import DataTypes
from pyflink.common import Configuration

cfg = Configuration()

cfg.set_string("table.exec.sort.default-limit", "2")


env_settings = (
    EnvironmentSettings.new_instance()
    .in_batch_mode()
    .with_configuration(cfg)
    .build()
)

table_env = TableEnvironment.create(env_settings)

# Generate minimal data
data = [
    (1, 10, 100.50),
    (2, 15, 200.75),
    (3, 20, 300.25),
    (4, 5, 400.00),
    (5, 18, 500.50),
    (6, 12, 600.75),
    (7, 25, 700.25)
]

schema = [
    "wr_item_sk",
    "wr_returning_hdemo_sk",
    "wr_reversed_charge"
]

# Create source table
source_table = table_env.from_elements(data, schema=schema)
table_env.create_temporary_view("web_returns", source_table)

# Query that causes the crash
autonode_7 = table_env.from_path("web_returns")
autonode_6 = autonode_7.filter(col('wr_returning_hdemo_sk') <= 16)
autonode_5 = autonode_6.select(col('wr_reversed_charge'))
autonode_4 = autonode_5.distinct()
autonode_3 = autonode_4.order_by(col('wr_reversed_charge'))
autonode_2 = autonode_3.group_by(col('wr_reversed_charge')).select(
    col('wr_reversed_charge').max.alias('wr_reversed_charge')
)
autonode_1 = autonode_2.limit(57)
sink = autonode_1.group_by(col('wr_reversed_charge')).select(
    col('wr_reversed_charge').sum.alias('wr_reversed_charge')
)

print(sink.explain())