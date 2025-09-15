from pathlib import Path
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.common import Configuration
import re
import sys
import json

def _build_create_table_sql(table_name, table_path, schemas):
    """Build the CREATE TABLE SQL statement for a given table"""
    schema_def = ', '.join(schemas[table_name])

    return f"""
    CREATE TEMPORARY TABLE {table_name} (
        {schema_def}
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{table_path}',
        'format' = 'csv',
        'csv.field-delimiter' = ',',
        'csv.ignore-first-line' = 'true',
        'csv.allow-comments' = 'false',
        'csv.ignore-parse-errors' = 'true'
    )
    """

def load_tpcds_schema(json_file):
    with open(json_file, "r") as f:
        data = json.load(f)
    return data


def extract_unoptimized_ast(plan_str: str) -> str:
    """
    Given Table.explain() output, slice out the '== Abstract Syntax Tree ==' section
    (i.e., the unoptimized logical plan).
    """
    start = plan_str.find("== Abstract Syntax Tree ==")
    if start < 0:
        return plan_str  # fallback; format may differ
    # end at the next section header if present
    m = re.search(r"\n== .*? ==\n", plan_str[start+1:])  # look after the first char to avoid matching the same header
    end = start + 1 + m.start() if m else len(plan_str)
    return plan_str[start:end].strip()

def optimized(args):
    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    schemas = load_tpcds_schema("pyflink-oracle-server/tpcds-schema.json")
    table_name_1 = "customer"
    table_name_2 = "customer_address"

    t_env.execute_sql(_build_create_table_sql(table_name_1, f"tpcds-csv/{table_name_1}", schemas))
    t_env.execute_sql(_build_create_table_sql(table_name_2, f"tpcds-csv/{table_name_2}", schemas))

    customer = t_env.from_path(table_name_1)
    address  = t_env.from_path(table_name_2)

    c1 = t_env.sql_query(f"SELECT COUNT(*) as row_count FROM {table_name_1}").execute().collect()
    c2 = t_env.sql_query(f"SELECT COUNT(*) as row_count FROM {table_name_1}").execute().collect()
    print(f"rows({table_name_1}): {list(c1)[0][0]}")
    print(f"rows({table_name_2}): {list(c2)[0][0]}")

    joined = (
        customer.join(address)
                .where(col("c_current_addr_sk") == col("ca_address_sk"))
                .select(
                    col("c_customer_id"),
                    col("c_first_name"),
                    col("ca_city"),
                    col("ca_state"),
                    col("ca_zip")
                )
    )

    full_plan = joined.explain()
    print("\n==== OPTIMIZED PLAN (AST) ====\n")
    print(extract_unoptimized_ast(full_plan))

def unoptimized(args):
    # --- (1) Build a TableEnvironment with optimizations dialed down ---
    cfg = Configuration()
    # Disable/limit common optimizer rewrites so the optimized plan stays close to the AST
    cfg.set_string("table.optimizer.multiple-input-enabled", "false")
    cfg.set_string("table.optimizer.join-reorder-enabled", "false")
    cfg.set_string("table.optimizer.reuse-sub-plan-enabled", "false")
    cfg.set_string("table.optimizer.reuse-source-enabled", "false")
    cfg.set_string("table.optimizer.runtime-filter.enabled", "false")
    cfg.set_string("table.optimizer.dynamic-filtering.enabled", "false")
    cfg.set_string("table.optimizer.adaptive-broadcast-join.strategy", "none")
    cfg.set_string("table.optimizer.skewed-join-optimization.strategy", "NONE")

    settings = (
        EnvironmentSettings.new_instance()
        .in_batch_mode()
        .with_configuration(cfg)
        .build()
    )
    t_env = TableEnvironment.create(settings)

    # --- (2) Load tables ---
    schemas = load_tpcds_schema("pyflink-oracle-server/tpcds-schema.json")
    table_name_1 = "customer"
    table_name_2 = "customer_address"

    t_env.execute_sql(_build_create_table_sql(table_name_1, f"tpcds-csv/{table_name_1}", schemas))
    t_env.execute_sql(_build_create_table_sql(table_name_2, f"tpcds-csv/{table_name_2}", schemas))

    customer = t_env.from_path(table_name_1)
    address  = t_env.from_path(table_name_2)

    # Sanity checks
    c1 = t_env.sql_query(f"SELECT COUNT(*) AS row_count FROM {table_name_1}").execute().collect()
    c2 = t_env.sql_query(f"SELECT COUNT(*) AS row_count FROM {table_name_2}").execute().collect()
    print(f"rows({table_name_1}): {list(c1)[0][0]}")
    print(f"rows({table_name_2}): {list(c2)[0][0]}")

    # --- (3) Join ---
    joined = (
        customer.join(address)
                .where(col("c_current_addr_sk") == col("ca_address_sk"))
                .select(
                    col("c_customer_id"),
                    col("c_first_name"),
                    col("ca_city"),
                    col("ca_state"),
                    col("ca_zip")
                )
    )

    # --- (4) Print ONLY the unoptimized plan (AST) ---
    # Table.explain() returns a string including:
    #   1) Abstract Syntax Tree (unoptimized logical plan)
    #   2) Optimized logical plan
    #   3) Physical execution plan
    # We'll slice out just the AST.
    full_plan = joined.explain()
    print("\n==== UNOPTIMIZED PLAN (AST) ====\n")
    print(extract_unoptimized_ast(full_plan))


def main(args):
    optimized(args)
    print("="*30)
    unoptimized(args)

main(sys.argv)

