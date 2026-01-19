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
import re
import difflib
import multiprocessing as mp
import polars as pl
from pathlib import Path
import shutil
import subprocess
import argparse

# ================ COVERAGE ====================

class RustCoverageMerger:
    def __init__(self, profraw_dir, llvm_profdata, output_dir):
        self.profraw_dir = Path(profraw_dir)
        self.output_dir = Path(output_dir)
        self.llvm_profdata = llvm_profdata

        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _timestamped_profdata_path(self):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.output_dir / f"coverage_{ts}.profdata"

    def merge_coverage(self):
        """Merge all .profraw files into a timestamped .profdata file."""
        profraw_files = list(self.profraw_dir.glob("*.profraw"))
        if not profraw_files:
            print("[coverage] No .profraw files found to merge")
            return None

        out_file = self._timestamped_profdata_path()

        cmd = [
            self.llvm_profdata,
            "merge",
            "-sparse",
            *[str(f) for f in profraw_files],
            "-o",
            str(out_file),
        ]

        print(f"[coverage] Merging {len(profraw_files)} profraw files")
        print(f"[coverage] Command: {' '.join(cmd)}")

        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            if result.stderr:
                print(f"[coverage] stderr: {result.stderr}")
            if out_file.exists():
                size = out_file.stat().st_size
                print(f"[coverage] Created {out_file} ({size:,} bytes)")
                return out_file
            else:
                print(f"[coverage] Output file not created: {out_file}")
                return None
        except subprocess.CalledProcessError as e:
            print(f"[coverage] Merge failed with exit code {e.returncode}")
            if e.stdout:
                print(f"[coverage] stdout: {e.stdout}")
            if e.stderr:
                print(f"[coverage] stderr: {e.stderr}")
            return None

    def merge_and_cleanup(self):
        """Merge coverage and delete the source profraw files."""
        profraw_files = list(self.profraw_dir.glob("*.profraw"))
        if not profraw_files:
            return None

        out_file = self.merge_coverage()
        if not out_file:
            return None

        deleted = 0
        for f in profraw_files:
            try:
                f.unlink()
                deleted += 1
            except OSError as e:
                print(f"[coverage] Failed to delete {f}: {e}")

        print(f"[coverage] Cleaned up {deleted} profraw files")
        return out_file


# =====================================================
def execute_in_process(code, result_queue, namespace):
    """Worker function that runs in subprocess"""
    try:
        exec(code, namespace, namespace)
        result_queue.put(
            {
                "success": True,
                "error_name": "",
                "error_message": "",
                "plan": namespace["final_plan"]
            }
        )
    except Exception as e:
        # Extract error name in the subprocess
        error_name = type(e).__name__
        error_msg = str(e)
        result_queue.put({"success": False, "error_name": error_name, "error_message": error_msg})

class GlobalState:
    host = 'localhost'
    port = 8890
    running = False
    tpcds_data_path = 'tpcds-csv'
    program_counter = 0
    counter_lock = threading.Lock()
    log_dir = 'server-log'

    table_schemas = None
    # Initialize Polars environment once
    polars_namespace = None

    timeout_seconds = 3
    coverage_profraw_dir = "/var/cov/profiles/live"
    coverage_merger = None


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

class PolarsFuzzingHandler(BaseHTTPRequestHandler):
# class PolarsFuzzingHandler():

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

    def setup_polars_environment(self):
        """Setup the polars environment once with all TPCDS CSV tables"""
        self._log_setup_start()

        try:
            tables = self._discover_and_log_tables()
            table_dataframes = self._load_all_tables(tables)
            namespace = self._build_namespace(table_dataframes)
            self._log_setup_success()
            return namespace
        except Exception as e:
            self._log_setup_failure(e)
            return None

    def _load_all_tables(self, tables):
        """Load all tables as polars dataframes"""
        table_dataframes = {}

        for table_name, table_path in tables.items():
            print(f"[{datetime.now()}] Loading table: {table_name}")

            # Get column names and dtypes from schema
            dtype_dict = self._get_dtype_dict(table_name)

            # Read all CSV files in the directory
            csv_files = glob.glob(os.path.join(table_path, "*.csv"))
            first_csv = csv_files[0]
            ddf = pl.read_csv(first_csv)

            table_dataframes[table_name] = ddf
            print(f"[{datetime.now()}]   Loaded {table_name} with {len(ddf.columns)} columns")

        return table_dataframes

    def _get_dtype_dict(self, table_name):
        """Convert schema definition to pandas dtype dictionary"""
        dtype_dict = {}

        for column_def in GLOBAL_STATE.table_schemas[table_name]:
            parts = column_def.split()
            if len(parts) < 2:
                continue

            col_name = parts[0]
            col_type = parts[1].upper()

            # Map SQL types to pandas dtypes
            if 'INT' in col_type:
                dtype_dict[col_name] = 'float64'  # Use float to handle nulls
            elif any(x in col_type for x in ['STRING', 'VARCHAR', 'CHAR']):
                dtype_dict[col_name] = 'object'
            elif any(x in col_type for x in ['DECIMAL', 'NUMERIC', 'FLOAT']):
                dtype_dict[col_name] = 'float64'
            elif any(x in col_type for x in ['DATE', 'TIMESTAMP']):
                dtype_dict[col_name] = 'object'  # Parse dates later if needed
            elif 'BOOL' in col_type:
                dtype_dict[col_name] = 'object'  # Use object for booleans with nulls
            else:
                dtype_dict[col_name] = 'object'

        return dtype_dict

    def _log_setup_start(self):
        """Log environment setup initialization"""
        print(f"[{datetime.now()}] Setting up Polars environment...")
        print(f"[{datetime.now()}] Python executable: {sys.executable}")
        print(f"[{datetime.now()}] TPCDS data path: {GLOBAL_STATE.tpcds_data_path}")

    def _discover_and_log_tables(self):
        """Discover TPCDS tables and log the results"""
        tables = self.discover_tpcds_tables()
        print(f"[{datetime.now()}] Discovered {len(tables)} TPCDS tables: {list(tables.keys())}")
        return tables

    def _build_namespace(self, table_dataframes):
        """Build the namespace dictionary with polars objects"""
        namespace = {
#             'pl': pl,
        }

        # Add each table as a variable
#         for table_name, ddf in table_dataframes.items():
#             namespace[table_name] = ddf

        return namespace

    def _log_setup_success(self):
        """Log successful environment setup"""
        print(f"[{datetime.now()}] Polars environment setup successful!")

    def _log_setup_failure(self, error):
        """Log environment setup failure"""
        print(f"[{datetime.now()}] Failed to setup Polars environment: {str(error)}")
        print(f"[{datetime.now()}] {traceback.format_exc()}")

    def execute_polars_code(self, received_code, code_type):
        """Execute the received code in the pre-initialized Polars namespace"""

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

    def setup_polars_namespace(self):
        global GLOBAL_STATE
        """Return a copy of the pre-initialized polars namespace"""
        if GLOBAL_STATE.polars_namespace is None:
            return None, "polars environment was not initialized properly"

        # Return a copy of the namespace for this execution
        return dict(GLOBAL_STATE.polars_namespace), None

    def _setup_execution_namespace(self):
        """Setup and validate Polars namespace"""
        namespace, setup_error = self.setup_polars_namespace()

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
        return self._try_exec_code(code, namespace, code_type)

    def _try_execute_code_unopt(self, code, namespace, code_type):
        # Match: final_plan = <something>.explain()
        pattern = r"final_plan\s*=\s*(.+?)\.explain\(\s*\)"

        match = re.search(pattern, code)
        if not match:
            # Nothing to rewrite
            return self._try_exec_code(code, namespace, code_type)

        var_expr = match.group(1)

        replacement = (
            "opt_flags = pl.QueryOptFlags()\n"
            "opt_flags.no_optimizations()\n"
            f"final_plan = {var_expr}.explain(optimizations=opt_flags)"
        )

        new_code = re.sub(pattern, replacement, code, count=1)

        return self._try_exec_code(new_code, namespace, code_type)

    def _try_exec_code(self, code, namespace, code_type):
        ns_local = {}
        final_code = code
        if code_type == "sql":
            final_code = f"""print(table_env.sql_query(\"\"\"{code}\"\"\").explain())"""
        return self._try_execute_code(final_code, namespace)

    def _try_execute_code(self, code, namespace):
        """Attempt code execution and capture any errors, including segfaults"""

        result_queue = mp.Queue()
        p = mp.Process(target=execute_in_process, args=(code, result_queue, namespace))
        p.start()
        p.join(timeout=GLOBAL_STATE.timeout_seconds)

        if p.is_alive():
            p.terminate()
            p.join(timeout=5)
            if p.is_alive():
                p.kill()
                p.join()
            return {
                "success": False,
                "error_name": "TimeoutError",
                "error_message": f"Code execution timed out after {GLOBAL_STATE.timeout_seconds} seconds"
            }, namespace

        if p.exitcode != 0:
            if p.exitcode == -11:  # SIGSEGV
                error_name = "SegmentationFault"
                error_message = "Process crashed with segmentation fault (SIGSEGV)"
            elif p.exitcode == -6:  # SIGABRT
                error_name = "Abort"
                error_message = "Process aborted (SIGABRT)"
            elif p.exitcode < 0:
                signal_num = -p.exitcode
                error_name = "ProcessCrash"
                error_message = f"Process killed by signal {signal_num}"
            else:
                error_name = "ProcessError"
                error_message = f"Process exited with code {p.exitcode}"
            return {"success": False, "error_name": error_name, "error_message": error_message}, namespace

        try:
            return result_queue.get_nowait(), namespace
        except:
            return {"success": False, "error_name": "UnknownError", "error_message": "No result received from subprocess"}, namespace

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
        Count the number of Python UDF calls in a Polars query plan.
        """
        if not plan:
            return 0

        # Count canonical Python UDF nodes
        return len(re.findall(r"\.python_udf\s*\(", plan))


    def _diff_success_results(self, result_opt, result_unopt):
        return self._diff_udf_counts(result_opt, result_unopt)


    def _diff_udf_counts(self, result_opt, result_unopt):
        """Compare stdout and stderr for two successful results."""
        # Extract output streams
        opt_plan = result_opt["plan"]
        unopt_plan = result_unopt["plan"]

        # Print plans
        print("=== OPTIMIZED PLAN ====")
        print(opt_plan)
        print("=== UNOPTIMIZED PLAN ====")
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
#         del result_opt['vars']
#         del result_unopt['vars']

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

            print(f"[COVERAGE] About to merge and clean")
            # Coverage
            GLOBAL_STATE.coverage_merger.merge_and_cleanup()

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
        print(f"[{datetime.now()}] Executing program #{program_number} with Polars template...")
        execution_result = self.execute_polars_code(code, code_type)
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
        GLOBAL_STATE.port = 8890
        GLOBAL_STATE.running = True
        GLOBAL_STATE.tpcds_data_path = 'tpcds-csv-5pc'
        GLOBAL_STATE.program_counter = 0
        GLOBAL_STATE.counter_lock = threading.Lock()
        GLOBAL_STATE.log_dir = 'oracle-servers/.logs/polars-server-log'

        self.setup_logging_directory(GLOBAL_STATE.log_dir)

        GLOBAL_STATE.table_schemas = self.load_tpcds_schema("oracle-servers/tpcds-schema.json")
        # Initialize polars environment once
        GLOBAL_STATE.polars_namespace = self.setup_polars_environment()

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

def run_server(port: int = 8890):
    """Start the HTTP server"""
    server_address = ('localhost', port)
    httpd = HTTPServer(server_address, PolarsFuzzingHandler)
    print(f"Starting server on http://localhost:{port}")
    print("Press Ctrl+C to stop the server")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        httpd.shutdown()

def get_coverage_out_dir_path(out_dir):
    coverage_path = Path(out_dir).parent / "coverage"
    coverage_path.mkdir(parents=True, exist_ok=True)
    return str(coverage_path)

def setup_coverage_profiling(args):
    profile_dir = Path(GLOBAL_STATE.coverage_profraw_dir)

    # Create directory if it doesn't exist
    profile_dir.mkdir(parents=True, exist_ok=True)

    # Remove existing files (if any)
    if profile_dir.exists():
        for f in profile_dir.iterdir():
            if f.is_file():
                f.unlink(missing_ok=True)
            else:
                shutil.rmtree(f, ignore_errors=True)

    # Set environment variable
    os.environ["LLVM_PROFILE_FILE"] = f"{profile_dir}/polars-%p-%m.profraw"

    GLOBAL_STATE.coverage_merger = RustCoverageMerger(profile_dir, get_llvm_profdata_path(), get_coverage_out_dir_path(args.out_dir))
    print(f"[COVERAGE] MERGER INITIALIZED: {GLOBAL_STATE.coverage_merger}")

def get_llvm_profdata_path():
    rustc_path = os.path.expanduser('~/.cargo/bin/rustc')
    target_libdir = subprocess.check_output([rustc_path, '--print', 'target-libdir'], text=True).strip()
    return os.path.join(os.path.dirname(target_libdir), 'bin', 'llvm-profdata')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Oracle server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--out-dir', '-d',
        type=str,
        default='/tmp/coverage',
        help='Directory containing profdata coverage files'
    )
    args = parser.parse_args()

    mp.set_start_method('spawn', force=True)
    setup_coverage_profiling(args)
    run_server()

