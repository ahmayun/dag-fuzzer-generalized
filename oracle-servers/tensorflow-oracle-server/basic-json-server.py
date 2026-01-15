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
import pandas as pd
from func_timeout import func_timeout, FunctionTimedOut


class GlobalState:
    host = 'localhost'
    port = 8890
    running = False
    tpcds_data_path = 'tpcds-csv-5pc'
    program_counter = 0
    counter_lock = threading.Lock()
    log_dir = 'tensorflow-server-log'

    table_schemas = None
    # Store loaded dataframes
    tensorflow_namespace = None
    timeout_seconds = 10


GLOBAL_STATE = GlobalState()


class MultilineJSONEncoder(json.JSONEncoder):
    def encode(self, obj):
        # First get the normal JSON representation
        result = super().encode(obj)
        return result


class TensorflowFuzzingHandler(BaseHTTPRequestHandler):

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

            # Save just the received code
            program_file = os.path.join(program_dir, 'program.py')
            with open(program_file, 'w', encoding='utf-8') as f:
                f.write(received_code)

            print(f"[{datetime.now()}] Saved logs for program {program_number} to {program_dir}")

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

    def setup_tensorflow_environment(self):
        """Setup the tensorflow environment once with all TPCDS CSV tables"""
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

    def _log_setup_start(self):
        """Log environment setup initialization"""
        print(f"[{datetime.now()}] Setting up tensorflow environment...")
        print(f"[{datetime.now()}] Python executable: {sys.executable}")
        print(f"[{datetime.now()}] TPCDS data path: {GLOBAL_STATE.tpcds_data_path}")

    def _discover_and_log_tables(self):
        """Discover TPCDS tables and log the results"""
        tables = self.discover_tpcds_tables()
        print(f"[{datetime.now()}] Discovered {len(tables)} TPCDS tables: {list(tables.keys())}")
        return tables

    def _load_all_tables(self, tables):
        """Load all tables as tensorflow dataframes"""
        table_dataframes = {}

        for table_name, table_path in tables.items():
            print(f"[{datetime.now()}] Loading table: {table_name}")

            # Get column names and dtypes from schema
            dtype_dict = self._get_dtype_dict(table_name)

            # Read all CSV files in the directory
            csv_files = glob.glob(os.path.join(table_path, "*.csv"))
            first_csv = csv_files[0]
            ddf = pd.read_csv(first_csv, dtype=dtype_dict)

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

    def _build_namespace(self, table_dataframes):
        """Build the namespace dictionary with tensorflow objects"""
        namespace = {
            'pd': pd,
        }

        # Add each table as a variable
        for table_name, ddf in table_dataframes.items():
            namespace[table_name] = ddf

        return namespace

    def _log_setup_success(self):
        """Log successful environment setup"""
        print(f"[{datetime.now()}] tensorflow environment setup successful!")

    def _log_setup_failure(self, error):
        """Log environment setup failure"""
        print(f"[{datetime.now()}] Failed to setup tensorflow environment: {str(error)}")
        print(f"[{datetime.now()}] {traceback.format_exc()}")

    def setup_tensorflow_namespace(self):
        global GLOBAL_STATE
        """Return a copy of the pre-initialized tensorflow namespace"""
        if GLOBAL_STATE.tensorflow_namespace is None:
            return None, "tensorflow environment was not initialized properly"

        # Return a copy of the namespace for this execution
        return dict(GLOBAL_STATE.tensorflow_namespace), None

    def extract_error_name(self, e):
        """Extract error name from exception"""
        error_name = type(e).__name__
        return error_name

    def execute_tensorflow_code(self, received_code):
        """Execute the received code in the pre-initialized tensorflow namespace"""

        try:
            namespace = self._setup_execution_namespace()
            if namespace is None:
                return self._create_setup_error_result()

            execution_result = self._execute_code_with_capture(received_code, namespace)
            self._log_execution_outputs(execution_result)
            return execution_result

        except Exception as e:
            return self._handle_critical_error(e)

    def _create_output_captures(self):
        """Create StringIO objects for capturing stdout and stderr"""
        return StringIO(), StringIO()

    def _setup_execution_namespace(self):
        """Setup and validate tensorflow namespace"""
        namespace, setup_error = self.setup_tensorflow_namespace()

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

    def _execute_code_with_capture(self, code, namespace):
        """Execute code with output capture and error handling"""
        print(f"[{datetime.now()}] Executing received code...")

        stdout_capture, stderr_capture = self._create_output_captures()
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = self._try_execute_code(code, namespace)

        result = {**result, "stdout": stdout_capture.getvalue(), "stderr": stderr_capture.getvalue()}

        return self._build_execution_result(code, result)

    def _try_execute_code(self, code, namespace):
        """Attempt code execution and capture any errors"""

        def execute_code():
            exec(code, namespace, namespace)

        try:
            func_timeout(GLOBAL_STATE.timeout_seconds, execute_code)
            return {"success": True, "error_name": "", "error_message": ""}
        except FunctionTimedOut:
                return {"success": False, "error_name": "TimeoutError",
                        "error_message": f"Code execution timed out after {GLOBAL_STATE.timeout_seconds} seconds"}
        except Exception as e:
            error_name = self.extract_error_name(e)
            error_msg = str(e)
            return {"success": False, "error_name": error_name, "error_message": error_msg}

    def _build_execution_result(self, code, result):
        """Build the execution result dictionary"""

        print(f"[{datetime.now()}] Code execution completed.")

        final_program = "# ======== Program ========\n" + \
                        f"{code}\n\n" + \
                        "# ======== Result ========\n" + \
                        f'"""\n{json.dumps(result, indent=2, cls=MultilineJSONEncoder)}\n"""\n\n'

        return {
            "success": result["success"],
            "error_name": result["error_name"],
            "error_message": result["error_message"],
            "stdout": result["stdout"],
            "stderr": result["stderr"],
            "final_program": final_program
        }

    def _log_execution_outputs(self, result):
        """Log stdout and stderr outputs from execution"""
        pass

    def _handle_critical_error(self, error):
        """Handle critical execution errors"""
        error_msg = f"Critical error during execution: {str(error)}\n{traceback.format_exc()}"
        self._log_critical_error(error_msg)

        return {
            'stdout': "",
            'stderr': error_msg,
            'return_code': -1,
            'success': False,
            'error_name': type(error).__name__,
            'error_message': error_msg,
            'final_program': ""
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
        code = self._extract_code_from_request(request_dict)

        try:
            program_number = self.get_next_program_number()
            self._log_program_received(program_number, code)

            execution_result = self._execute_and_process(program_number, code)
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
        return request_dict['code']

    def _log_program_received(self, program_number, code):
        """Log details of received program"""
        print(f"\n{'='*60}")
        print(f"PROGRAM #{program_number} RECEIVED")
        print(f"Length: {len(code)} characters")
        print(f"{'='*60}")
        print(code)
        print(f"{'='*60}")

    def _execute_and_process(self, program_number, code):
        """Execute code and process results"""
        print(f"[{datetime.now()}] Executing program #{program_number}...")
        execution_result = self.execute_tensorflow_code(code)
        self._save_execution_logs(program_number, code, execution_result)
        return execution_result

    def _save_execution_logs(self, program_number, code, execution_result):
        """Save program execution logs to disk"""
        self.save_program_logs(program_number, code)

    def _log_execution_results(self, program_number, execution_result):
        """Log detailed execution results"""
        success = execution_result['success']

        self._print_results_header(program_number, success)
        self._print_results_footer()

    def _print_results_header(self, program_number, success):
        """Print execution results header"""
        print(f"\n{'='*60}")
        print(f"EXECUTION RESULTS FOR PROGRAM #{program_number}")
        print(f"Success: {success}")
        print(f"Logs saved to: {os.path.join(GLOBAL_STATE.log_dir, str(program_number))}")

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
        GLOBAL_STATE.log_dir = 'tf-server-log'

        self.setup_logging_directory(GLOBAL_STATE.log_dir)

        GLOBAL_STATE.table_schemas = self.load_tpcds_schema("oracle-servers/tpcds-schema.json")
        # Initialize tensorflow environment once
        GLOBAL_STATE.tensorflow_namespace = self.setup_tensorflow_environment()

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
    httpd = HTTPServer(server_address, TensorflowFuzzingHandler)
    print(f"Starting server on http://localhost:{port}")
    print("Press Ctrl+C to stop the server")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        httpd.shutdown()


if __name__ == '__main__':
    run_server()