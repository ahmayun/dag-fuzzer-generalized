from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table import expressions as F

# Create table environment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# Create two simple tables
table_env.execute_sql("""
    CREATE TABLE t1 (
        a INT
    ) WITH (
        'connector' = 'datagen',
        'number-of-rows' = '3'
    )
""")

table_env.execute_sql("""
    CREATE TABLE t2 (
        c INT
    ) WITH (
        'connector' = 'datagen',
        'number-of-rows' = '3'
    )
""")

# Minimal correlated subquery with LIMIT that triggers the error
print("Attempting correlated subquery with LIMIT...")
try:
    result = table_env.sql_query("""
        SELECT t1.a
        FROM t1
        WHERE EXISTS (
            SELECT t1.a
            FROM t2
            WHERE t1.a IS NOT NULL
            LIMIT 10
        )
    """)
    result.execute().print()
except Exception as e:
    print(f"Error: {type(e).__name__}")
    print(f"Message: {str(e)}")
print("="*60)
print("Attempting with TABLE API...")
try:
    t1 = table_env.from_path("t1")
    t2 = table_env.from_path("t2")
    t1.select(col("a")).where(F.exists(t2.select(1)))

except Exception as e:
 print(f"Error: {type(e).__name__}")
 print(f"Message: {str(e)}")