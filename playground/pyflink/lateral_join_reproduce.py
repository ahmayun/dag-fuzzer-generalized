from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# Create the reason table
table_env.execute_sql("""
    CREATE TABLE reason (
        r_reason_sk INT,
        r_reason_id STRING,
        r_reason_desc STRING
    ) WITH (
        'connector' = 'datagen',
        'number-of-rows' = '5'
    )
""")

# Create the store table
table_env.execute_sql("""
    CREATE TABLE store (
        s_store_sk INT,
        s_store_id STRING,
        s_floor_space INT,
        s_hours STRING,
        s_manager STRING,
        s_country STRING
    ) WITH (
        'connector' = 'datagen',
        'number-of-rows' = '5'
    )
""")

# Create the call_center table
table_env.execute_sql("""
    CREATE TABLE call_center (
        cc_call_center_sk INT,
        cc_call_center_id STRING
    ) WITH (
        'connector' = 'datagen',
        'number-of-rows' = '5'
    )
""")

# Execute the problematic query
print("Executing LATERAL join query with scalar subquery...")
try:
    result = table_env.sql_query("""
        select
          ref_0.r_reason_desc as c0
        from
          reason as ref_0,
          lateral (select
                ref_1.s_floor_space as c0
              from
                store as ref_1
              where (ref_0.r_reason_id is NULL or 4 is not NULL))
    """)

    result.execute().print()

except Exception as e:
    print(f"Error: {type(e).__name__}")
    print(f"Message: {str(e)}")