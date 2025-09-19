from typing import Callable, Optional, Dict
from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from pyflink.common import Configuration
from pyflink.table.expressions import col
import re
import json

# --- small helper to slice the AST section out of explain() output ---
def _extract_ast(plan: str) -> str:
    return plan
    start = plan.find("== Optimized Execution Plan ==")
    if start < 0:
        # fallback if header text changes across versions
        return plan
    m = re.search(r"\n== .*? ==\n", plan[start+1:])
    end = start + 1 + (m.start() if m else len(plan) - (start + 1))
    return plan[start:end].strip()

def _build_env(disable_opts: bool, session_opts: Optional[Dict[str, str]] = None) -> TableEnvironment:
    if disable_opts:
        cfg = Configuration()
        cfg.set_string("table.optimizer.join-reorder-enabled", "false")
        cfg.set_string("table.optimizer.sql2rel.project-merge.enabled", "false")
        cfg.set_string("table.optimizer.union-all-as-breakpoint-enabled", "false")
        cfg.set_string("table.optimizer.incremental-agg-enabled", "false")
        cfg.set_string("table.optimizer.reuse-sub-plan-enabled", "false")
        cfg.set_string("table.optimizer.reuse-source-enabled", "false")
        cfg.set_string("table.optimizer.multiple-input-enabled", "false")
        cfg.set_string("table.optimizer.runtime-filter.enabled", "false")
        cfg.set_string("table.optimizer.dynamic-filtering.enabled", "false")

        cfg.set_string("table.optimizer.distinct-agg.split.enabled", "false")
        cfg.set_string("table.optimizer.local-global-agg-enabled", "false")

        # Source Push-down Optimizations - Turn OFF
        cfg.set_string("table.optimizer.source.predicate-pushdown-enabled", "false")
        cfg.set_string("table.optimizer.source.aggregate-pushdown-enabled", "false")
        cfg.set_string("table.optimizer.source.partition-pushdown-enabled", "false")
        cfg.set_string("table.optimizer.source.projection-pushdown-enabled", "false")

        # Window Optimizations - Turn OFF
        cfg.set_string("table.optimizer.window-agg.enable-incremental-agg", "false")
        cfg.set_string("table.optimizer.window-join.enable-sliding-window", "false")

        # Code Generation - Turn OFF
        cfg.set_string("table.exec.compiled.enabled", "false")
        cfg.set_string("table.exec.codegen.enabled", "false")
        cfg.set_string("table.exec.codegen.length.max", "1")

        # Spill and Compression - Turn OFF
        cfg.set_string("table.exec.spill-compression.enabled", "false")
        cfg.set_string("table.exec.sort.async-merge-enabled", "false")

        # Early/Late Fire - Turn OFF
        cfg.set_string("table.exec.emit.early-fire.enabled", "false")
        cfg.set_string("table.exec.emit.late-fire.enabled", "false")
        cfg.set_string("table.exec.emit.allow-lateness", "0 ms")

        # Advanced Optimizations - Turn OFF
        cfg.set_string("table.optimizer.cbo-enabled", "false")
        cfg.set_string("table.optimizer.bushy-join-reorder.enabled", "false")
        cfg.set_string("table.optimizer.decorrelation-enabled", "false")
        cfg.set_string("table.optimizer.predicate-pushdown-enabled", "false")
        cfg.set_string("table.optimizer.project-pushdown-enabled", "false")

        # Parallelism and Resource Optimizations - Turn OFF/Minimize
        cfg.set_string("table.exec.resource.default-parallelism", "1")
        cfg.set_string("table.exec.resource.external-shuffle-mode", "ALL_EDGES_BLOCKING")

        # State Optimizations - Turn OFF
        cfg.set_string("table.exec.state.ttl", "0 ms")
        cfg.set_string("table.exec.mini-batch.enabled", "false")

        # Sink Optimizations - Turn OFF
        cfg.set_string("table.exec.sink.upsert-materialize", "NONE")
        cfg.set_string("table.exec.sink.not-null-enforcer", "ERROR")

        # Watermark Optimizations - Turn OFF
        cfg.set_string("table.exec.source.idle-timeout", "0 ms")

        # Memory and Buffer Optimizations - Minimize
        cfg.set_string("table.exec.sort.default-limit", "1")
        cfg.set_string("table.exec.sort.max-num-file-handles", "1")

        # Legacy Features - Enable (disables new optimizations)
        cfg.set_string("table.exec.legacy-cast-behaviour", "ENABLED")

        cfg.set_string("table.optimizer.runtime-filter.max-build-data-size", "0")

        # Dynamic Table Options - Turn OFF
        cfg.set_string("table.dynamic-table-options.enabled", "false")

        # Additional optimizations that might exist - Turn OFF
        cfg.set_string("table.optimizer.adaptive-join.enabled", "false")
        cfg.set_string("table.optimizer.bushy-tree-enabled", "false")
        cfg.set_string("table.optimizer.cbo.stats.enabled", "false")
        cfg.set_string("table.optimizer.constraint-propagation-enabled", "false")
        cfg.set_string("table.optimizer.expression-simplification-enabled", "false")
        cfg.set_string("table.optimizer.filter-pushdown-enabled", "false")
        cfg.set_string("table.optimizer.limit-pushdown-enabled", "false")
        cfg.set_string("table.optimizer.partition-pruning-enabled", "false")
        cfg.set_string("table.optimizer.projection-pushdown-enabled", "false")
        cfg.set_string("table.optimizer.subquery-decorrelation-enabled", "false")
        cfg.set_string("table.optimizer.operator-fusion-codegen.enabled", "false")

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

    # Apply any user-provided session options
    if session_opts:
        for k, v in session_opts.items():
            t_env.execute_sql(f"SET '{k}' = '{v}'")

    return t_env

def run(
    init_fn,
    work_fn: Callable[[TableEnvironment], Table],
    optimized=True,
    print_plan: bool = True,
    execute: bool = False,
    session_opts: Optional[Dict[str, str]] = None,
) -> Table:


    t_env = _build_env(disable_opts=(not optimized), session_opts=session_opts)
    init_fn(t_env)

    table = work_fn(t_env)
    if not isinstance(table, Table):
        raise TypeError("work_fn must return a pyflink.table.Table")

    plan = table.explain()
    if print_plan:
        print(f"\n==== {'' if optimized else 'UN'}OPTIMIZED PLAN (AST) ====\n")
        print(_extract_ast(plan))

    if execute:
        table.execute().print()
    return table


def build_join(t_env: TableEnvironment) -> Table:
    c = t_env.from_path("customer")
    a = t_env.from_path("customer_address")
    return (
        c.join(a)
         .where(col("c_current_addr_sk") == col("ca_address_sk"))
         .select(col("c_customer_id"), col("c_first_name"), col("ca_city"), col("ca_state"), col("ca_zip"))
    )

def build_double_scan_union_api(t_env: TableEnvironment) -> Table:
    c1 = (
        t_env.from_path("customer")
            .filter(~col("c_first_name").is_null())             # <-- use NOT is_null()
            .select(col("c_customer_id").alias("id"))
    )

    c2 = (
        t_env.from_path("customer")
            .filter(col("c_customer_sk") > lit(0))
            .select(col("c_customer_id").alias("id"))
    )

    return c1.union_all(c2)

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

def build_init_fn(table_names, schemas):
    def init_fn(env: TableEnvironment):
        for table_name in table_names:
            env.execute_sql(_build_create_table_sql(table_name, f"tpcds-csv/{table_name}", schemas))

    return init_fn

schemas = load_tpcds_schema("pyflink-oracle-server/tpcds-schema.json")
table_names = ["customer", "customer_address"]

run(build_init_fn(table_names, schemas), build_join, optimized=False, execute=False)
run(build_init_fn(table_names, schemas), build_join, optimized=True, execute=False)
