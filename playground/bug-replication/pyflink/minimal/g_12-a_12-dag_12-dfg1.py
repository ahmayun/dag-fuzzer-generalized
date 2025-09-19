from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from pyflink.common import Configuration
from pyflink.table.expressions import col

cfg = Configuration()


# The program runs fine if I set any default limit
cfg.set_string("table.exec.sort.default-limit", "10000")

settings = (
   EnvironmentSettings.new_instance()
   .in_batch_mode()
   .with_configuration(cfg)
   .build()
)

table_env = TableEnvironment.create(settings)

data = [
   ("Los Angeles", 9.50, "Premium", 1),
   ("Houston", 8.25, "Basic", 2),
   ("New York", 10.00, "Standard", 1),
   ("Chicago", 8.25, "Premium", 1)
]

source_table = table_env.from_elements(
   data,
   schema=["cc_city", "cc_tax_percentage", "cc_mkt_class", "extra"]
)

ordered = source_table.order_by(col('cc_city'))
result = ordered.group_by(col('cc_tax_percentage')).select(col('extra').min.alias('cc_mkt_class'))

print(result.to_pandas())
