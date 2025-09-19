from typing import Callable, Optional, Dict
from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from pyflink.common import Configuration
from pyflink.table.expressions import col
import re
import json
import sys
import os

def create_env(disable_opts: bool) -> TableEnvironment:
    if disable_opts:
        cfg = Configuration()
#         cfg.set_string("table.optimizer.join-reorder-enabled", "false")
#         cfg.set_string("table.optimizer.sql2rel.project-merge.enabled", "false")
#         cfg.set_string("table.optimizer.union-all-as-breakpoint-enabled", "false")
#         cfg.set_string("table.optimizer.incremental-agg-enabled", "false")
#         cfg.set_string("table.optimizer.reuse-sub-plan-enabled", "false")
#         cfg.set_string("table.optimizer.reuse-source-enabled", "false")
#         cfg.set_string("table.optimizer.multiple-input-enabled", "false")
#         cfg.set_string("table.optimizer.runtime-filter.enabled", "false")
#         cfg.set_string("table.optimizer.dynamic-filtering.enabled", "false")
#         cfg.set_string("table.optimizer.distinct-agg.split.enabled", "false")
#         cfg.set_string("table.optimizer.local-global-agg-enabled", "false")
#         cfg.set_string("table.optimizer.source.predicate-pushdown-enabled", "false")
#         cfg.set_string("table.optimizer.source.aggregate-pushdown-enabled", "false")
#         cfg.set_string("table.optimizer.source.partition-pushdown-enabled", "false")
#         cfg.set_string("table.optimizer.source.projection-pushdown-enabled", "false")
#         cfg.set_string("table.optimizer.window-agg.enable-incremental-agg", "false")
#         cfg.set_string("table.optimizer.window-join.enable-sliding-window", "false")
#         cfg.set_string("table.exec.compiled.enabled", "false")
#         cfg.set_string("table.exec.codegen.enabled", "false")
#         cfg.set_string("table.exec.codegen.length.max", "1")
#         cfg.set_string("table.exec.spill-compression.enabled", "false")
#         cfg.set_string("table.exec.sort.async-merge-enabled", "false")
#         cfg.set_string("table.exec.emit.early-fire.enabled", "false")
#         cfg.set_string("table.exec.emit.late-fire.enabled", "false")
#         cfg.set_string("table.exec.emit.allow-lateness", "0 ms")
#         cfg.set_string("table.optimizer.cbo-enabled", "false")
#         cfg.set_string("table.optimizer.bushy-join-reorder.enabled", "false")
#         cfg.set_string("table.optimizer.decorrelation-enabled", "false")
#         cfg.set_string("table.optimizer.predicate-pushdown-enabled", "false")
#         cfg.set_string("table.optimizer.project-pushdown-enabled", "false")
#         cfg.set_string("table.exec.resource.default-parallelism", "1")
#         cfg.set_string("table.exec.resource.external-shuffle-mode", "ALL_EDGES_BLOCKING")
#         cfg.set_string("table.exec.state.ttl", "0 ms")
#         cfg.set_string("table.exec.mini-batch.enabled", "false")
#         cfg.set_string("table.exec.sink.upsert-materialize", "NONE")
#         cfg.set_string("table.exec.sink.not-null-enforcer", "ERROR")
#         cfg.set_string("table.exec.source.idle-timeout", "0 ms")
        cfg.set_string("table.exec.sort.default-limit", "10000")
#         cfg.set_string("table.exec.sort.max-num-file-handles", "1")
#         cfg.set_string("table.exec.legacy-cast-behaviour", "ENABLED")
#         cfg.set_string("table.optimizer.runtime-filter.max-build-data-size", "0")
#         cfg.set_string("table.dynamic-table-options.enabled", "false")
#         cfg.set_string("table.optimizer.adaptive-join.enabled", "false")
#         cfg.set_string("table.optimizer.bushy-tree-enabled", "false")
#         cfg.set_string("table.optimizer.cbo.stats.enabled", "false")
#         cfg.set_string("table.optimizer.constraint-propagation-enabled", "false")
#         cfg.set_string("table.optimizer.expression-simplification-enabled", "false")
#         cfg.set_string("table.optimizer.filter-pushdown-enabled", "false")
#         cfg.set_string("table.optimizer.limit-pushdown-enabled", "false")
#         cfg.set_string("table.optimizer.partition-pruning-enabled", "false")
#         cfg.set_string("table.optimizer.projection-pushdown-enabled", "false")
#         cfg.set_string("table.optimizer.subquery-decorrelation-enabled", "false")
#         cfg.set_string("table.optimizer.operator-fusion-codegen.enabled", "false")

        settings = (
            EnvironmentSettings.new_instance()
            .in_batch_mode()
            .with_configuration(cfg)
            .build()
        )
        t_env = TableEnvironment.create(settings)
    else:
        # Fresh, default-optimized env
        t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

    return t_env

def load_tpcds_schema(json_file):
    with open(json_file, "r") as f:
        data = json.load(f)
    return data

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

def load_tables(t_env, table_names, schemas):
    for table_name in table_names:
        t_env.execute_sql(_build_create_table_sql(table_name, f"tpcds-csv/{table_name}", schemas))

def write_output_log(out_path, e):
    with open(out_path, "w") as f:
        f.write(f"{e}")

def print_intermediate_rdd(rdd, line):
    print(f"=== {line} ===")
    print(rdd.to_pandas())
    print("="*30)
    pass

def program(table_env):
    return g30(table_env)

def original_program(table_env):
    autonode_5 = table_env.from_path("call_center")
    print_intermediate_rdd(autonode_5, 'table_env.from_path("call_center")')

    autonode_4 = autonode_5.distinct()
    print_intermediate_rdd(autonode_4, 'autonode_5.distinct()')

    autonode_3 = autonode_4.distinct()
    print_intermediate_rdd(autonode_3, 'autonode_4.distinct()')

    autonode_2 = autonode_3.order_by(col('cc_city'))
    print_intermediate_rdd(autonode_2, 'autonode_3.order_by(col("cc_city"))')

    autonode_1 = autonode_2.group_by(col('cc_tax_percentage')).select(col('cc_mkt_class').min.alias('cc_mkt_class'))
    print_intermediate_rdd(autonode_1, "autonode_2.group_by(col('cc_tax_percentage')).select(col('cc_mkt_class').min.alias('cc_mkt_class'))")

    sink = autonode_1.order_by(col('cc_mkt_class'))
    print_intermediate_rdd(sink, "autonode_1.order_by(col('cc_mkt_class'))")

    return sink

def aliased_program(table_env):
    autonode_5 = table_env.from_path("call_center")
    uid = "t4223"

    columns = autonode_5.get_schema().get_field_names()
    select_expressions = [f"{col} AS {col}_{uid}" for col in columns]
    autonode_5 = autonode_5.select(*select_expressions)

    print_intermediate_rdd(autonode_5, 'table_env.from_path("call_center")')

    autonode_4 = autonode_5.distinct()
    print_intermediate_rdd(autonode_4, 'autonode_5.distinct()')

    autonode_3 = autonode_4.distinct()
    print_intermediate_rdd(autonode_3, 'autonode_4.distinct()')

    autonode_2 = autonode_3.order_by(col('cc_city'))
    print_intermediate_rdd(autonode_2, 'autonode_3.order_by(col("cc_city"))')

    autonode_1 = autonode_2.group_by(col('cc_tax_percentage')).select(col('cc_mkt_class').min.alias('cc_mkt_class'))
    print_intermediate_rdd(autonode_1, "autonode_2.group_by(col('cc_tax_percentage')).select(col('cc_mkt_class').min.alias('cc_mkt_class'))")

    sink = autonode_1.order_by(col('cc_mkt_class'))
    print_intermediate_rdd(sink, "autonode_1.order_by(col('cc_mkt_class'))")

    return sink

def g30(table_env):
    #.select(col("s_store_sk"), col("s_store_id"), col("s_rec_start_date"), col("s_rec_end_date"), col("s_closed_date_sk"), col("s_store_name"), col("s_number_employees"), col("s_floor_space"), col("s_hours"), col("s_manager"), col("s_market_id"), col("s_geography_class"), col("s_market_desc"), col("s_market_manager"), col("s_division_id"), col("s_division_name"), col("s_company_id"), col("s_company_name"), col("s_street_number"), col("s_street_name"), col("s_street_type"), col("s_suite_number"), col("s_city"), col("s_county"), col("s_state"), col("s_zip"), col("s_country"), col("s_gmt_offset"), col("s_tax_precentage"))
    #.select(col('s_gmt_offset'), col('s_manager'), col('s_street_name'))
    autonode_7 = table_env.from_path("store").select(col("s_store_sk"), col("s_store_id"), col("s_manager"), col("s_street_name"), col("s_gmt_offset"))
    print_intermediate_rdd(autonode_7, "selected columns")
    autonode_6 = autonode_7.order_by(col('s_manager'))
    autonode_5 = autonode_6.group_by(col('s_street_name')).select(col('s_gmt_offset').count.alias('s_gmt_offset'))
    print(autonode_5.explain())
    return autonode_5

def simplified_program(table_env):
    # Create table with sample data
    data = [
        ("Los Angeles", 9.50, "Premium", 1),
        ("Houston", 8.25, "Basic", 2),
        ("New York", 10.00, "Standard", 1),
        ("Chicago", 8.25, "Premium", 1)
    ]

    source_table = table_env.from_elements(
        data,
        schema=["cc_city", "cc_tax_percentage", "cc_mkt_class", "dummy"]
    )

    ordered = source_table.order_by(col('cc_city'))
    result = ordered.group_by(col('cc_tax_percentage')).select(col('cc_mkt_class').min.alias('cc_mkt_class'))

    return result

def main(args):
    schemas = load_tpcds_schema("pyflink-oracle-server/tpcds-schema.json")
    tables = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address",
              "customer_demographics", "date_dim", "household_demographics", "income_band", "inventory", "item",
              "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse",
              "web_page", "web_returns", "web_sales","web_site"]

    case_study_path = "target/case-study"
    os.makedirs(case_study_path, exist_ok=True)
    opt_out_path = f"{case_study_path}/opt.txt"
    unopt_out_path = f"{case_study_path}/unopt.txt"

    try:
        t_env_opt = create_env(disable_opts=False)
        load_tables(t_env_opt, tables, schemas)
        rdd = program(t_env_opt)
        plan = rdd.explain()
        write_output_log(opt_out_path, plan)
        print(f"PASS: Optimized query successful. Plan written to {opt_out_path}")
        print(rdd.to_pandas())
    except Exception as e:
        print(f"ERROR: Optimized query failed, writing log to {opt_out_path}")
        write_output_log(opt_out_path, e)

    print("="*30)

    try:
        t_env_unopt = create_env(disable_opts=True)
        load_tables(t_env_unopt, tables, schemas)
        rdd = program(t_env_unopt)
        plan = rdd.explain()
        write_output_log(unopt_out_path, plan)
        print(f"PASS: Unoptimized query successful. Plan written to {unopt_out_path}")
        print(rdd.to_pandas())
    except Exception as e:
        print(f"ERROR: Unoptimized query failed, writing log to {unopt_out_path}")
        write_output_log(unopt_out_path, e)

if __name__ == "__main__":
    main(sys.argv)