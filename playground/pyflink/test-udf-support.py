from pyflink.table.udf import udaf
from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col, call
import pandas as pd

# Define a vectorized function that works on pandas Series
def pandas_count_vectorized(values: pd.Series) -> int:
    return len(values)

# Create the vectorized UDAF
try:
    vectorized_count = udaf(
        pandas_count_vectorized,
        result_type=DataTypes.BIGINT(),
        func_type="pandas"
    )
    print("Vectorized Pandas UDAF creation: SUCCESS")
except Exception as e:
    print(f"Vectorized Pandas UDAF creation: FAILED - {e}")

# Create table environment
env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
t_env = TableEnvironment.create(env_settings)

# Register the function
try:
    t_env.drop_temporary_function("pandas_count")
    print("Dropped existing function")
except:
    print("No existing function to drop")

try:
    t_env.create_temporary_function("pandas_count", vectorized_count)
    print("Function registration: SUCCESS")
except Exception as e:
    print(f"Function registration: FAILED - {e}")

# Create test data and table
test_data = [
    ('A', 1),
    ('A', 2),
    ('B', 3),
    ('B', 4),
    ('A', 5)
]

try:
    table = t_env.from_elements(test_data, ['category', 'value'])
    print("Test table creation: SUCCESS")

    # Use the UDAF
    result = table.group_by(col('category')).select(
        col('category'),
        call('pandas_count', col('value')).alias('count')
    )
    print("Query creation: SUCCESS")
    print(result.explain())

except Exception as e:
    print(f"Query setup: FAILED - {e}")