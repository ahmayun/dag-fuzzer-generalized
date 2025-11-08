from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import udf
from pyflink.table.types import DataTypes
from pyflink.common import Configuration
from pyflink.table.udf import AggregateFunction, udaf
import pandas as pd

# Create table environment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)


table_env.execute_sql(f"""
        CREATE TEMPORARY TABLE catalog_page (
            cp_catalog_page_sk INT,
            cp_catalog_page_id STRING,
            cp_start_date_sk INT,
            cp_end_date_sk INT,
            cp_department STRING,
            cp_catalog_number INT,
            cp_catalog_page_number INT,
            cp_description STRING,
            cp_type STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'tpcds-csv/catalog_page',
            'format' = 'csv',
            'csv.field-delimiter' = ',',
            'csv.ignore-first-line' = 'true',
            'csv.allow-comments' = 'false',
            'csv.ignore-parse-errors' = 'true'
        )
""")

# # Create source table
# source_table = table_env.from_elements(data, schema=schema)
#
# # Register as temporary view to mimic catalog_page
# table_env.create_temporary_view("catalog_page", source_table)

def preloaded_aggregation(values: pd.Series) -> float:
    return values.std()
try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass
preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")
table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)
autonode_5 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_4 = autonode_5.limit(66)
autonode_3 = autonode_4.distinct()
autonode_2 = autonode_3.order_by(col('cp_department_node_5'))
autonode_1 = autonode_2.distinct()
sink = autonode_1.limit(28)
print(sink.explain())