from pyflink.table import *
from pyflink.table.expressions import *
from pyflink.table.udf import udf
from pyflink.table.types import DataTypes
from pyflink.common import Configuration
from pyflink.table.udf import AggregateFunction, udaf
from pyflink.table import DataTypes
import pandas as pd

def custom_aggregation(values: pd.Series):
    return values.product()

custom_udf_agg = udaf(custom_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

cfg = Configuration()

settings = (
   EnvironmentSettings.new_instance()
   .in_batch_mode()
   .with_configuration(cfg)
   .build()
)

table_env = TableEnvironment.create(settings)
table_env.create_temporary_function("custom_udf_agg", custom_udf_agg)

data = [
    (101,),
    (102,),
    (101,),
    (103,)
]

schema = ["A"]

# Create the source table
source_table = table_env.from_elements(
    data,
    schema=schema
)

t = source_table.group_by(col('A')).select(col('A').avg.alias('A'))
t = t.distinct()
t = t.group_by(col('A')).select(col('A').max.alias('A'))
t = t.group_by(col('A')).select(call('custom_udf_agg', col('A')).alias('A'))
# print(t.explain())
t.execute().print()
