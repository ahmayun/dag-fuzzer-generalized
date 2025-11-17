#!/usr/bin/env python3

import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any
import sys
from datetime import datetime
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
import traceback
import os
import glob
import threading
import json
from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment, Table
import re
import difflib

class GlobalState:
    host = 'localhost'
    port = 8888
    running = False
    tpcds_data_path = 'tpcds-csv'
    program_counter = 0
    counter_lock = threading.Lock()
    log_dir = 'server-log'

    table_schemas = None
    # Initialize Flink environment once
    flink_namespace = None


GLOBAL_STATE = GlobalState()


class MultilineJSONEncoder(json.JSONEncoder):
    def encode(self, obj):
        # First get the normal JSON representation
        result = super().encode(obj)

        # Then replace escaped newlines with actual newlines in string values
        # This regex finds quoted strings containing \n and replaces \n with actual newlines
        def replace_newlines(match):
            string_content = match.group(1)
            # Replace \\n with actual newlines, but be careful about already escaped backslashes
            formatted = string_content.replace('\\n', '\n')
            return f'"{formatted}"'

        # Find all quoted strings that contain \n
        result = re.sub(r'"([^"]*\\n[^"]*)"', replace_newlines, result)
        return result

class FlinkFuzzingHandler(BaseHTTPRequestHandler):
# class FlinkFuzzingHandler():

    def setup_logging_directory(self, log_dir):
        """Create the server-log directory if it doesn't exist"""
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            print(f"[{datetime.now()}] Created logging directory: {log_dir}")
        else:
            print(f"[{datetime.now()}] Using existing logging directory: {log_dir}")

    def get_next_program_number(self):
        global GLOBAL_STATE
        """Get the next program sequence number (thread-safe)"""
        with GLOBAL_STATE.counter_lock:
            current_number = GLOBAL_STATE.program_counter
            GLOBAL_STATE.program_counter += 1
            return current_number

    def create_program_log_directory(self, program_number):
        """Create a directory for this program's logs"""
        program_dir = os.path.join(GLOBAL_STATE.log_dir, str(program_number))
        os.makedirs(program_dir, exist_ok=True)
        return program_dir

    def save_program_logs(self, program_number, received_code):
        """Save the program and its outputs to log files"""
        try:
            program_dir = self.create_program_log_directory(program_number)

            # Save just the received code (not the template)
            program_file = os.path.join(program_dir, 'program.py')
            with open(program_file, 'w', encoding='utf-8') as f:
                f.write(received_code)

            print(f"[{datetime.now()}] Saved logs for program {program_number} to {program_dir}")
            print(f"[{datetime.now()}] Received code saved (without template)")

        except Exception as e:
            print(f"[{datetime.now()}] Failed to save logs for program {program_number}: {e}")

    def discover_tpcds_tables(self):
        """Discover all TPCDS tables from the CSV data directory"""
        if not self._validate_data_path():
            return {}

        discovered_items = self._get_directory_items()
        tables = {}

        for item in discovered_items:
            table_path = self._build_table_path(item)

            if self._is_valid_table_directory(table_path):
                if self._has_schema_definition(item):
                    tables[item] = os.path.abspath(table_path)
                    self._log_table_found(item, tables[item])
                else:
                    self._log_missing_schema_warning(item)

        return tables

    def _validate_data_path(self):
        """Check if the TPCDS data path exists"""
        if not os.path.exists(GLOBAL_STATE.tpcds_data_path):
            print(f"Warning: TPCDS data path '{GLOBAL_STATE.tpcds_data_path}' does not exist")
            return False
        return True

    def _get_directory_items(self):
        """Get all items from the TPCDS data directory"""
        return os.listdir(GLOBAL_STATE.tpcds_data_path)

    def _build_table_path(self, item):
        """Build the full path for a table directory"""
        return os.path.join(GLOBAL_STATE.tpcds_data_path, item)

    def _is_valid_table_directory(self, table_path):
        """Check if path is a directory containing CSV files"""
        if not os.path.isdir(table_path):
            return False

        return self._contains_csv_files(table_path)

    def _contains_csv_files(self, table_path):
        """Check if directory contains any CSV files"""
        csv_files = glob.glob(os.path.join(table_path, "*.csv"))
        return len(csv_files) > 0

    def _has_schema_definition(self, table_name):
        """Check if schema is defined for the table"""
        return table_name in GLOBAL_STATE.table_schemas

    def _log_table_found(self, table_name, table_path):
        """Log when a valid table is discovered"""
        print(f"Found TPCDS table: {table_name} -> {table_path}")

    def _log_missing_schema_warning(self, table_name):
        """Log warning when table has no schema definition"""
        print(f"Warning: No schema defined for table '{table_name}'. Skipping.")

    def setup_flink_environment(self, catalog_name=None):
        """Setup the Flink environment once with all TPCDS CSV tables"""
        self._log_setup_start()

        try:
            tables = self._discover_and_log_tables()
            opt_table_env = self._create_table_environment()
            unopt_table_env = self._create_unopt_table_env()
            self._register_all_tables(opt_table_env, tables, catalog_name)
            self._register_all_tables(unopt_table_env, tables, catalog_name)
#             self._verify_all_tables(opt_table_env, tables, catalog_name)

            namespace = self._build_namespace(opt_table_env, unopt_table_env)
            self._log_setup_success()
            return namespace

        except Exception as e:
            self._log_setup_failure(e)
            return None

    def _log_setup_start(self):
        """Log environment setup initialization"""
        print(f"[{datetime.now()}] Setting up Flink environment...")
        print(f"[{datetime.now()}] Python executable: {sys.executable}")
        print(f"[{datetime.now()}] TPCDS data path: {GLOBAL_STATE.tpcds_data_path}")

    def _discover_and_log_tables(self):
        """Discover TPCDS tables and log the results"""
        tables = self.discover_tpcds_tables()
        print(f"[{datetime.now()}] Discovered {len(tables)} TPCDS tables: {list(tables.keys())}")
        return tables

    def _create_table_environment(self):
        """Create and return a Flink table environment"""
        from pyflink.table import EnvironmentSettings, TableEnvironment

        env_settings = EnvironmentSettings.in_batch_mode()
        return TableEnvironment.create(env_settings)

    def _create_unopt_table_env(self):
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

        return TableEnvironment.create(settings)

    def _register_all_tables(self, table_env, tables, catalog_name=None):
        """Register all discovered tables with the table environment"""
        if catalog_name:
            self.setup_catalog_and_database(table_env, catalog_name)

        for table_name, table_path in tables.items():
            self._register_single_table(table_env, table_name, table_path, catalog_name)

    def _register_single_table(self, table_env, table_name, table_path, catalog_name=None):
        """Register a single table with the Flink environment"""
        print(f"[{datetime.now()}] Registering table: {table_name}")

        create_table_sql = self._build_create_table_sql(table_name, table_path, catalog_name)
        table_env.execute_sql(create_table_sql)

    def setup_catalog_and_database(self, table_env, catalog_name):
        """Set up catalog and database structure"""
        table_env.execute_sql(f"""
            CREATE DATABASE IF NOT EXISTS {catalog_name}
        """)

        table_env.use_database(catalog_name)

    def _build_create_table_sql(self, table_name, table_path, catalog_name=None):
        """Build the CREATE TABLE SQL statement for a given table"""
        schema_def = ', '.join(GLOBAL_STATE.table_schemas[table_name])
        full_table_name = f"{catalog_name}.{table_name}" if catalog_name else table_name

        return f"""
        CREATE TEMPORARY TABLE {full_table_name} (
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

    def _verify_all_tables(self, table_env, tables, catalog_name=None):
        """Verify all registered tables are working correctly"""
        print(f"[{datetime.now()}] Verifying TPCDS tables...")

        for table_name in tables.keys():
            full_table_name = f"{catalog_name}.{table_name}" if catalog_name else table_name
            self._verify_single_table(table_env, full_table_name)

        print(f"[{datetime.now()}] Table verification complete.")

    def _verify_single_table(self, table_env, table_name):
        """Verify a single table by checking row count and schema"""
        try:
            print(f"[{datetime.now()}] Testing table: {table_name}")

            row_count = self._get_table_row_count(table_env, table_name)
            print(f"[{datetime.now()}]   {table_name}: {row_count} rows")

            self._log_table_schema(table_env, table_name)

        except Exception as e:
            print(f"[{datetime.now()}]   ERROR with {table_name}: {e}")

    def _get_table_row_count(self, table_env, table_name):
        """Get the row count for a specific table"""
        result = table_env.sql_query(f"SELECT COUNT(*) as row_count FROM {table_name}")
        count_result = result.execute().collect()
        return list(count_result)[0][0]

    def _log_table_schema(self, table_env, table_name):
        """Log the schema information for a table"""
        schema_result = table_env.sql_query(f"SELECT * FROM {table_name} LIMIT 1")
        schema_names = schema_result.get_schema().get_field_names()

        display_columns = schema_names[:5]
        suffix = '...' if len(schema_names) > 5 else ''
        print(f"[{datetime.now()}]   {table_name} columns: {display_columns}{suffix}")

    def _build_namespace(self, opt_table_env, unopt_table_env):
        """Build the namespace dictionary with Flink objects"""
        from pyflink.table import EnvironmentSettings, TableEnvironment

        return {
            'opt_table_env': opt_table_env,
            'unopt_table_env': unopt_table_env,
            'EnvironmentSettings': EnvironmentSettings,
            'TableEnvironment': TableEnvironment,
        }

    def _log_setup_success(self):
        """Log successful environment setup"""
        print(f"[{datetime.now()}] Flink environment setup successful!")

    def _log_setup_failure(self, error):
        """Log environment setup failure"""
        print(f"[{datetime.now()}] Failed to setup Flink environment: {str(error)}")
        print(f"[{datetime.now()}] {traceback.format_exc()}")

    def setup_flink_namespace(self):
        global GLOBAL_STATE
        """Return a copy of the pre-initialized Flink namespace"""
        if GLOBAL_STATE.flink_namespace is None:
            return None, "Flink environment was not initialized properly"

        # Return a copy of the namespace for this execution
        return dict(GLOBAL_STATE.flink_namespace), None

    def extract_error_name(self, e):
        if hasattr(e, 'java_exception'):
            # Get the actual Java exception class name
            java_error_name = e.java_exception.getClass().getName()

            try:
                return java_error_name.split(".")[-1]
            except:
                return java_error_name  # org.apache.flink.table.api.ValidationException
        else:
            # Fallback to Python exception name
            error_name = type(e).__name__
            return error_name

    def execute_flink_code(self, received_code, code_type):
        """Execute the received code in the pre-initialized Flink namespace"""

        try:
            namespace = self._setup_execution_namespace()
            if namespace is None:
                return self._create_setup_error_result()

            execution_result = self._execute_code_with_capture(received_code, namespace, code_type)
            self._log_execution_outputs(execution_result)
            return execution_result

        except Exception as e:
            return self._handle_critical_error(e)

    def _create_output_captures(self):
        """Create StringIO objects for capturing stdout and stderr"""
        return StringIO(), StringIO()

    def _setup_execution_namespace(self):
        """Setup and validate Flink namespace"""
        namespace, setup_error = self.setup_flink_namespace()

        if setup_error:
            self._log_setup_error(setup_error)
            return None

        return namespace

    def _log_setup_error(self, setup_error):
        """Log namespace setup errors"""
        print(f"[{datetime.now()}] Setup error occurred:")
        print(setup_error)

    def _create_setup_error_result(self):
        """Create result dictionary for setup errors"""
        return {
            'stdout': '',
            'stderr': 'Namespace setup failed',
            'return_code': -1,
            'success': False,
            'error_message': '',
            'error_name': '',
            'final_program': ""
        }

    def _execute_code_with_capture(self, code, namespace, code_type):
        """Execute code with output capture and error handling"""
        print(f"[{datetime.now()}] Executing received code...")

        stdout_capture_opt, stderr_capture_opt = self._create_output_captures()
        with redirect_stdout(stdout_capture_opt), redirect_stderr(stderr_capture_opt):
            result_opt, ns_opt = self._try_execute_code_as_is(code, namespace, code_type)
        result_opt = {**result_opt, "stdout": stdout_capture_opt.getvalue(), "stderr": stderr_capture_opt.getvalue(), 'vars':{**ns_opt}}

        stdout_capture_unopt, stderr_capture_unopt = self._create_output_captures()
        with redirect_stdout(stdout_capture_unopt), redirect_stderr(stderr_capture_unopt):
            result_unopt, ns_unopt = self._try_execute_code_unopt(code, namespace, code_type)
        result_unopt = {**result_unopt, "stdout": stdout_capture_unopt.getvalue(), "stderr": stderr_capture_unopt.getvalue(), 'vars':{**ns_opt}}

        return self._build_execution_result(code, result_opt, result_unopt)

    def _try_execute_code_as_is(self, code, namespace, code_type):
        """Attempt code execution and capture any errors"""
        namespace['table_env'] = namespace['opt_table_env']
        return self._try_exec_code(code, namespace, code_type)

    def _try_execute_code_unopt(self, code, namespace, code_type):
        """Attempt code execution and capture any errors"""
        namespace['table_env'] = namespace['unopt_table_env']
        return self._try_exec_code(code, namespace, code_type)

    def _try_exec_code(self, code, namespace, code_type):
        ns_local = {}
        final_code = code
        if code_type == "sql":
            final_code = f"""print(table_env.sql_query(\"\"\"{code}\"\"\").explain())"""
        try:
            exec(final_code, namespace, namespace)
            return {"success": True, "error_name": "", "error_message": ""}, namespace
        except Exception as e:
            error_name = self.extract_error_name(e)
#             error_msg = f"{str(e)}\n{traceback.format_exc()}"
            error_msg = str(e)
            return {"success": False, "error_name": error_name, "error_message": error_msg}, namespace

    def diff_outputs(self, result_opt, result_unopt):
        """
        Compare two result dictionaries based on success status and content.

        result_dict format:
            {"success": True, "error_name": "", "error_message": "", "stdout": "", "stderr": ""}

        Returns:
            dict: Comparison result with is_same, result_name, and result_details
        """
        # Check for success status mismatch
        if self._has_success_mismatch(result_opt, result_unopt):
            return self._create_mismatch_result(result_opt, result_unopt)

        # Both have same success status
        if not result_opt["success"] and not result_unopt["success"]:
            return self._diff_error_results(result_opt, result_unopt)
        elif result_opt["success"] and result_unopt["success"]:
            return self._diff_success_results(result_opt, result_unopt)

        # This should not happen due to the first check, but added for completeness
        return self._create_mismatch_result(result_opt, result_unopt)


    def _has_success_mismatch(self, result_opt, result_unopt):
        """Check if there's a mismatch in success status between results."""
        return result_opt["success"] != result_unopt["success"]


    def _create_mismatch_result(self, result_opt, result_unopt):
        """Create a mismatch exception result with both dictionaries in details."""
        return {
            "is_same": False,
            "result_name": "MismatchException",
            "result_details": {
                "opt": result_opt,
                "unopt": result_unopt
            }
        }


    def _diff_error_results(self, result_opt, result_unopt):
        """Compare two failed results based on their error names."""
        opt_error = result_opt.get("error_name", "")
        unopt_error = result_unopt.get("error_name", "")

        if opt_error == unopt_error:
            return {
                "is_same": True,
                "result_name": opt_error,
                "result_details": {
                    "opt": result_opt,
                    "unopt": result_unopt
                }
            }
        else:
            return {
                "is_same": False,
                "result_name": "MismatchException",
                "result_details": {
                    "opt_error": opt_error,
                    "unopt_error": unopt_error
                }
            }

    def _extract_ast(self, plan: str) -> str:
        start = plan.find("== Optimized Execution Plan ==")
        if start < 0:
            # fallback if header text changes across versions
            return plan
        m = re.search(r"\n== .*? ==\n", plan[start+1:])
        end = start + 1 + (m.start() if m else len(plan) - (start + 1))
        return plan[start:end].strip()

    def _extract_ast_lines(self, plan_string):
        """Extract lines after '== Optimized Execution Plan ==' marker."""
        if not plan_string:
            return []

        lines = plan_string.strip().split('\n')
        ast_start_idx = self._find_ast_marker_index(lines)

        if ast_start_idx is None:
            return []

        return [line.rstrip() for line in lines[ast_start_idx:] if line.strip()]

    def _find_ast_marker_index(self, lines):
        """Find the index after the AST marker line."""
        for i, line in enumerate(lines):
            if "== Optimized Execution Plan ==" in line:
                return i + 1  # Start after the marker line
        return None

    def _are_plans_identical(self, opt_lines, unopt_lines):
        """Check if two plan line lists are identical."""
        return opt_lines == unopt_lines

    def _generate_diff_output(self, opt_lines, unopt_lines):
        """Generate and return unified diff as a list of strings."""

        return list(difflib.unified_diff(
            unopt_lines,
            opt_lines,
            fromfile='unoptimized_plan',
            tofile='optimized_plan',
            lineterm=''
        ))


    def _print_diff_results(self, is_same, diff_lines=None):
        """Print the diff comparison results to console."""
        print("\n=== DIFF RESULT ===")

        if is_same:
            print("Plans are identical!")
        else:
            print("Plans differ:")
            if diff_lines:
                print("\n".join(diff_lines))

    def _create_result_dict(self, same_plans, opt_plan, unopt_plan, opt_lines, unopt_lines, diff_lines=None):
        """Create the standardized result dictionary."""
        result_name = "Success" if same_plans else "MismatchException"

        return {
            "is_same": same_plans,
            "result_name": result_name,
            "result_details": {
                "opt_plan": opt_plan,
                "unopt_plan": unopt_plan,
                "diff_lines": "\n".join(diff_lines) if diff_lines else ""
            }
        }

    def count_udfs_in_plan(self, plan):
        """
        Count the number of UDF calls in a PyFlink optimized query plan.

        Args:
            plan (str): The query plan string

        Returns:
            int: Number of UDF calls found in the plan
        """
        if not plan:
            return 0

        udf_count = 0

        # Pattern to match Python UDF function references
        # These typically appear as PythonScalarFunction, PythonTableFunction, etc.
        python_function_pattern = r'\*org\.apache\.flink\.table\.functions\.python\.Python\w+Function[^*]*\*'

        # Find all Python function references
        python_functions = re.findall(python_function_pattern, plan)
        udf_count += len(python_functions)

        # Alternative pattern for other UDF representations that might appear
        # Look for PythonCalc nodes which typically contain UDF calls
        pythoncalc_pattern = r'PythonCalc\('
        pythoncalc_matches = re.findall(pythoncalc_pattern, plan)

        # If we found PythonCalc nodes but no Python function patterns,
        # it might indicate UDFs in a different format
        if len(pythoncalc_matches) > 0 and udf_count == 0:
            # Count PythonCalc nodes as they typically represent UDF operations
            udf_count = len(pythoncalc_matches)

        # Look for other potential UDF patterns like custom function calls
        # This catches cases where UDFs might be represented differently
        custom_udf_pattern = r'(?:udf|UDF)\w*\('
        custom_udfs = re.findall(custom_udf_pattern, plan, re.IGNORECASE)
        udf_count += len(custom_udfs)

        return udf_count

    def _diff_success_results(self, result_opt, result_unopt):
        df_diff = self._diff_dfs(result_opt, result_unopt)
        if df_diff['is_same'] == False:
            print("Returning df_diff")
            print(df_diff)
            print('-----')
            return df_diff

        if df_diff['is_same'] == None:
            print("ERROR FAILED DF DIFF")

        return self._diff_udf_counts(result_opt, result_unopt)

    def _diff_dfs(self, result_opt, result_unopt):
        opt_df = result_opt['vars']['sink']
        unopt_df = result_unopt['vars']['sink']

        try:
            # Convert DataFrames to Pandas for comparison
            opt_pandas = opt_df.to_pandas()
            unopt_pandas = unopt_df.to_pandas()

            # Add row_id to preserve order
            opt_pandas['row_id'] = range(len(opt_pandas))
            unopt_pandas['row_id'] = range(len(unopt_pandas))

            # Check if column names match
            if set(opt_pandas.columns) != set(unopt_pandas.columns):
                return {
                    "is_same": False,
                    "result_name": "MismatchException",
                    "result_details": {
                        "error": "Column mismatch",
                        "opt_columns": list(opt_pandas.columns),
                        "unopt_columns": list(unopt_pandas.columns),
                        "missing_in_opt": list(set(unopt_pandas.columns) - set(opt_pandas.columns)),
                        "missing_in_unopt": list(set(opt_pandas.columns) - set(unopt_pandas.columns))
                    }
                }

            # Reorder columns to match
            unopt_pandas = unopt_pandas[opt_pandas.columns]

            # Perform set difference in both directions
            opt_merged = opt_pandas.merge(unopt_pandas, indicator=True, how='outer')

            # Rows only in opt_df
            only_in_opt = opt_merged[opt_merged['_merge'] == 'left_only'].drop('_merge', axis=1)
            # Rows only in unopt_df
            only_in_unopt = opt_merged[opt_merged['_merge'] == 'right_only'].drop('_merge', axis=1)

            # Check if both differences are empty
            if only_in_opt.empty and only_in_unopt.empty:
                return {
                    "is_same": True,
                    "result_name": "Success",
                    "result_details": {
                        "message": "DFs match",
                        "row_count": len(opt_pandas),
                        "column_count": len(opt_pandas.columns) - 1  # Exclude row_id
                    }
                }
            else:
                # Limit rows for readability
                max_rows_to_show = 100

                return {
                    "is_same": False,
                    "result_name": "MismatchException",
                    "result_details": {
                        "error": "Outputs don't match",
                        "rows_only_in_opt": only_in_opt.head(max_rows_to_show).to_dict('records'),
                        "rows_only_in_unopt": only_in_unopt.head(max_rows_to_show).to_dict('records'),
                        "count_only_in_opt": len(only_in_opt),
                        "count_only_in_unopt": len(only_in_unopt),
                        "total_mismatches": len(only_in_opt) + len(only_in_unopt)
                    }
                }

        except Exception as e:
            return {
                "is_same": None,
                "result_name": "MismatchException",
                "result_details": {
                    "error": "Exception during comparison",
                    "exception_type": type(e).__name__,
                    "exception_message": str(e)
                }
            }


    def _diff_udf_counts(self, result_opt, result_unopt):
        """Compare stdout and stderr for two successful results."""
        # Extract output streams
        opt_stdout, opt_stderr = result_opt["stdout"], result_opt["stderr"]
        unopt_stdout, unopt_stderr = result_unopt["stdout"], result_unopt["stderr"]

        # Print plans
        print("=== OPTIMIZED PLAN ====")
        opt_plan = self._extract_ast(opt_stdout)
        print(opt_plan)
        print("=== UNOPTIMIZED PLAN ====")
        unopt_plan = self._extract_ast(unopt_stdout)
        print(unopt_plan)

        n_opt_udfs, n_unopt_udfs = list(map(lambda p: self.count_udfs_in_plan(p), [opt_plan, unopt_plan]))

        # Return structured result
#         return self._create_result_dict(same_plans, opt_plan, unopt_plan, opt_lines, unopt_lines, diff_lines)
        same_udfs = n_opt_udfs == n_unopt_udfs
        result_name = "Success" if same_udfs else "MismatchException"
        return {
            "is_same": same_udfs,
            "result_name": result_name,
            "result_details": {
                "opt_plan": opt_plan,
                "unopt_plan": unopt_plan,
                "count_diff": {"n_opt_udfs": n_opt_udfs,"n_unopt_udfs": n_unopt_udfs}
            }
        }

    def _build_execution_result(self, code, result_opt, result_unopt):
        """Build the execution result dictionary"""

        print(f"[{datetime.now()}] Code execution completed.")
#         print(f"[{datetime.now()}] result_opt = {result_opt}.")
#         print(f"[{datetime.now()}] result_unopt = {result_unopt}.")

        diff_result = self.diff_outputs(result_opt, result_unopt)
        del result_opt['vars']
        del result_unopt['vars']

        final_program = "# ======== Program ========\n" + \
                        f"{code}\n\n" + \
                        "# ======== Details ========\n" + \
                        f'"""\n{json.dumps(diff_result, indent=2, cls=MultilineJSONEncoder)}\n"""\n\n'
        return {
            "opt": { **result_opt },
            "unopt": { **result_unopt },
            "success": diff_result["is_same"],
            "error_name": diff_result["result_name"],
            "error_message": "",
            "diff": diff_result["result_details"],
            "final_program": final_program
        }

    def _log_execution_outputs(self, result):
        """Log stdout and stderr outputs from execution"""
#         self._log_stdout_output(result['stdout'])
#         self._log_stderr_output(result['stderr'])
        pass

    def _log_stdout_output(self, stdout_output):
        """Log stdout output if present"""
        if stdout_output:
            print(f"[{datetime.now()}] STDOUT:")
            print("-" * 40)
            print(stdout_output)
            print("-" * 40)
        else:
            print(f"[{datetime.now()}] No stdout output")

    def _log_stderr_output(self, stderr_output):
        """Log stderr output if present"""
        if stderr_output:
            print(f"[{datetime.now()}] STDERR:")
            print("-" * 40)
            print(stderr_output)
            print("-" * 40)
        else:
            print(f"[{datetime.now()}] No stderr output")

    def _handle_critical_error(self, error):
        """Handle critical execution errors"""
        error_msg = f"Critical error during execution: {str(error)}\n{traceback.format_exc()}"
        self._log_critical_error(error_msg)

        return {
            'stdout': "",
            'stderr': error_msg,
            'return_code': -1,
            'success': False,
            'namespace': None,
            'error_name': ''
        }

    def _log_critical_error(self, error_msg):
        """Log critical errors with formatted output"""
        print(f"[{datetime.now()}] CRITICAL ERROR:")
        print("-" * 50)
        print(error_msg)
        print("-" * 50)

    def handle_execute_code(self, request_dict):
        """Handle individual client connections"""
        self._log_handler_start()
        code, code_type = self._extract_code_from_request(request_dict)

        try:
            program_number = self.get_next_program_number()
            self._log_program_received(program_number, code)

            execution_result = self._execute_and_process(program_number, code, code_type)
            self._log_execution_results(program_number, execution_result)

            return execution_result
        except Exception as e:
            import traceback
            self._log_handler_error(e)
            return {"fatal_error": str(e), "stack_trace": traceback.format_exc()}


    def _log_handler_start(self):
        """Log the start of code execution handler"""
        print(f"[{datetime.now()}] Running handle_execute_code()...")

    def _extract_code_from_request(self, request_dict):
        """Extract code data from request dictionary"""
        return request_dict['code'], (request_dict['code_type'] if 'code_type' in request_dict else 'api')

    def _log_program_received(self, program_number, code):
        """Log details of received program"""
        print(f"\n{'='*60}")
        print(f"PROGRAM #{program_number} RECEIVED")
        print(f"Length: {len(code)} characters")
        print(f"{'='*60}")
        print(code)
        print(f"{'='*60}")

    def _execute_and_process(self, program_number, code, code_type):
        """Execute code and process results"""
        print(f"[{datetime.now()}] Executing program #{program_number} with Flink template...")
        execution_result = self.execute_flink_code(code, code_type)
        self._save_execution_logs(program_number, code, execution_result)
        return execution_result

    def _save_execution_logs(self, program_number, code, execution_result):
        """Save program execution logs to disk"""
        self.save_program_logs(program_number, code)

    def _log_execution_results(self, program_number, execution_result):
        """Log detailed execution results"""
        success = execution_result['success']
        return_code = 0 # execution_result['return_code']
#         stderr_output = execution_result['stderr']
        namespace = execution_result.get('namespace', {})

        self._print_results_header(program_number, success, return_code)
#         self._print_error_details(stderr_output)
        self._print_namespace_info(namespace)
        self._print_results_footer()

    def _print_results_header(self, program_number, success, return_code):
        """Print execution results header"""
        print(f"\n{'='*60}")
        print(f"EXECUTION RESULTS FOR PROGRAM #{program_number}")
        print(f"Success: {success}")
        print(f"Return Code: {return_code}")
        print(f"Logs saved to: {os.path.join(GLOBAL_STATE.log_dir, str(program_number))}")

    def _print_error_details(self, stderr_output):
        """Print error details if present"""
        if stderr_output:
            print(f"Error Details:")
            print("-" * 30)
            print(stderr_output)
            print("-" * 30)

    def _print_namespace_info(self, namespace):
        """Print available namespace variables"""
        available_vars = list(namespace.keys()) if namespace else 'None'
        print(f"Available variables in namespace: {available_vars}")

    def _print_results_footer(self):
        """Print execution results footer"""
        print(f"{'='*60}\n")

    def _log_handler_error(self, error):
        """Log handler execution error"""
        print(f"[{datetime.now()}] Error handling client {error}")

    def do_POST(self):
        try:
            # Parse the incoming request
            request_dict = self.parse_json_request()

            # Process the request (business logic)
            response_dict = self.process_request(request_dict)

            # Convert response to JSON
            response_json = self.dict_to_json(response_dict)

            # Send JSON response back to sender
            self.send_json_response(response_json)

        except Exception as e:
            self.send_error_response(str(e))

    def parse_json_request(self) -> Dict[str, Any]:
        """Parse incoming HTTP request and return as dictionary"""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        request_dict = json.loads(post_data.decode('utf-8'))
        return request_dict

    def get_tables(self):
        return self.convert_schema_format(GLOBAL_STATE.table_schemas)

    def handle_get_tables(self, request_dict: Dict[str, Any]):
        return { "tables": self.get_tables() }

    def load_tpcds_schema(self, json_file):
        with open(json_file, "r") as f:
            data = json.load(f)
        return data

    def convert_schema_format(self, input_dict):
        """Convert schema dictionary to standardized format"""
        result = []

        for table_name, columns in input_dict.items():
            table_schema = self._create_base_table_schema(table_name)

            for column_def in columns:
                column_schema = self._process_column_definition(column_def)
                if column_schema:
                    table_schema["columns"].append(column_schema)

            result.append(table_schema)

        return result

    def _create_base_table_schema(self, table_name):
        """Create base table schema structure"""
        return {
            "identifier": table_name,
            "columns": [],
            "metadata": {
                "source": "database",
                "owner": "admin"
            }
        }

    def _process_column_definition(self, column_def):
        """Process a single column definition string"""
        parts = column_def.split()

        if len(parts) < 2:
            return None

        column_name = parts[0]
        data_type_raw = ' '.join(parts[1:])

        return self._create_column_schema(column_name, data_type_raw)

    def _create_column_schema(self, column_name, data_type_raw):
        """Create column schema with all attributes"""
        return {
            "name": column_name,
            "dataType": self._parse_data_type(data_type_raw),
            "isNullable": self._is_nullable_column(column_name, data_type_raw),
            "isKey": self._is_key_column(column_name)
        }

    def _parse_data_type(self, type_str):
        """Parse data type string and return standardized type"""
        type_str = type_str.upper()

        if 'INT' in type_str:
            return 'integer'
        elif self._is_string_type(type_str):
            return 'string'
        elif self._is_decimal_type(type_str):
            return 'decimal'
        elif self._is_date_type(type_str):
            return 'date'
        elif 'BOOL' in type_str:
            return 'boolean'
        else:
            return 'string'  # Default fallback

    def _is_string_type(self, type_str):
        """Check if type represents a string data type"""
        string_types = ['STRING', 'VARCHAR', 'CHAR']
        return any(string_type in type_str for string_type in string_types)

    def _is_decimal_type(self, type_str):
        """Check if type represents a decimal/numeric data type"""
        decimal_types = ['DECIMAL', 'NUMERIC', 'FLOAT']
        return any(decimal_type in type_str for decimal_type in decimal_types)

    def _is_date_type(self, type_str):
        """Check if type represents a date/time data type"""
        date_types = ['DATE', 'TIMESTAMP']
        return any(date_type in type_str for date_type in date_types)

    def _is_key_column(self, column_name):
        """Determine if a column is likely a key based on naming patterns"""
        name_lower = column_name.lower()
        key_patterns = ['_id', '_sk', '_key', 'id_', 'key_']

        return (any(pattern in name_lower for pattern in key_patterns) or
                name_lower == 'id')

    def _is_nullable_column(self, column_name, data_type):
        """Determine if a column should be nullable based on patterns"""
        if self._is_key_column(column_name):
            return False

        if self._is_required_field(column_name):
            return False

        if self._is_optional_field(column_name):
            return True

        return True  # Default: most fields can be nullable

    def _is_required_field(self, column_name):
        """Check if column name indicates a required field"""
        name_lower = column_name.lower()
        required_patterns = ['name', 'type', 'class', 'status']
        return any(pattern in name_lower for pattern in required_patterns)

    def _is_optional_field(self, column_name):
        """Check if column name indicates an optional field"""
        name_lower = column_name.lower()
        optional_patterns = ['desc', 'description', 'manager', 'suite', 'county']
        return any(pattern in name_lower for pattern in optional_patterns)

    def handle_load_data(self, request_dict: Dict[str, Any]) -> Dict[str, Any]:
        global GLOBAL_STATE
        GLOBAL_STATE.host = 'localhost'
        GLOBAL_STATE.port = 8888
        GLOBAL_STATE.running = True
        GLOBAL_STATE.tpcds_data_path = 'tpcds-csv-5pc'
        GLOBAL_STATE.program_counter = 0
        GLOBAL_STATE.counter_lock = threading.Lock()
        GLOBAL_STATE.log_dir = 'server-log'

        catalog_name = None
        self.setup_logging_directory(GLOBAL_STATE.log_dir)

        GLOBAL_STATE.table_schemas = self.load_tpcds_schema("pyflink-oracle-server/tpcds-schema.json")
        # Initialize Flink environment once

        if 'catalog_name' in request_dict:
            catalog_name = request_dict['catalog_name']

        GLOBAL_STATE.flink_namespace = self.setup_flink_environment(catalog_name)

        return {"success": True}

    def process_request(self, request_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Process the request dictionary and return response dictionary"""
        message_type = request_dict.get('message_type')

        if message_type == 'get_tables':
            return self.handle_get_tables(request_dict)
        elif message_type == 'execute_code':
            return self.handle_execute_code(request_dict)
        elif message_type == 'load_data':
            return self.handle_load_data(request_dict)
        else:
            return {"error": f"Unknown message type: {message_type}"}

    def dict_to_json(self, response_dict: Dict[str, Any]) -> str:
        """Convert dictionary to JSON string"""
        return json.dumps(response_dict, indent=2)

    def send_json_response(self, json_string: str):
        """Send JSON response back to the sender"""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Content-length', str(len(json_string.encode('utf-8'))))
        self.end_headers()
        self.wfile.write(json_string.encode('utf-8'))

    def send_error_response(self, error_message: str):
        """Send error response back to sender"""
        error_dict = {"error": error_message}
        error_json = self.dict_to_json(error_dict)

        self.send_response(500)
        self.send_header('Content-type', 'application/json')
        self.send_header('Content-length', str(len(error_json.encode('utf-8'))))
        self.end_headers()
        self.wfile.write(error_json.encode('utf-8'))

def run_server(port: int = 8888):
    """Start the HTTP server"""
    server_address = ('localhost', port)
    httpd = HTTPServer(server_address, FlinkFuzzingHandler)
    print(f"Starting server on http://localhost:{port}")
    print("Press Ctrl+C to stop the server")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        httpd.shutdown()


def main():
    o = FlinkFuzzingHandler()

    result_opt = {
        "stdout": """
    == Optimized Execution Plan ==
    LogicalProject(d_qoy=[$1])
    +- LogicalAggregate(group=[{4}], EXPR$0=[COUNT($10)])
       +- LogicalFilter(condition=[>=($8, 16)])
          +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
    """,
        "stderr": "No errors detected.\n"
    }

    # Example unoptimized result (with different plan structure)
    result_unopt = {
        "stdout": """
    == Optimized Execution Plan ==
    LogicalProject(d_qoy=[$1])
    +- LogicalAggregate(group=[{4}], EXPR$0=[COUNT($10)])
       +- LogicalSort(fetch=[53])
          +- LogicalFilter(condition=[>=($8, 16)])
             +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
    """,
        "stderr": "Warning: Query not optimized.\n"
    }
    d = o._diff_success_results(result_opt, result_unopt)
    print(json.dumps(d, indent=2))

if __name__ == '__main__':
    run_server()
#     main()

