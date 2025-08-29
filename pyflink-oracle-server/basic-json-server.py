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

class FlinkFuzzingHandler(BaseHTTPRequestHandler):

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

    def save_program_logs(self, program_number, received_code, stdout_output, stderr_output):
        """Save the program and its outputs to log files"""
        try:
            program_dir = self.create_program_log_directory(program_number)

            # Save just the received code (not the template)
            program_file = os.path.join(program_dir, 'program.py')
            with open(program_file, 'w', encoding='utf-8') as f:
                f.write(received_code)

            # Save stdout
            stdout_file = os.path.join(program_dir, 'stdout.txt')
            with open(stdout_file, 'w', encoding='utf-8') as f:
                f.write(stdout_output)

            # Save stderr
            stderr_file = os.path.join(program_dir, 'stderr.txt')
            with open(stderr_file, 'w', encoding='utf-8') as f:
                f.write(stderr_output)

            print(f"[{datetime.now()}] Saved logs for program {program_number} to {program_dir}")
            print(f"[{datetime.now()}] Received code saved (without template)")

        except Exception as e:
            print(f"[{datetime.now()}] Failed to save logs for program {program_number}: {e}")

    def discover_tpcds_tables(self):
        """Discover all TPCDS tables from the CSV data directory"""
        tables = {}

        if not os.path.exists(GLOBAL_STATE.tpcds_data_path):
            print(f"Warning: TPCDS data path '{GLOBAL_STATE.tpcds_data_path}' does not exist")
            return tables

        # Get all subdirectories (table names)
        for item in os.listdir(GLOBAL_STATE.tpcds_data_path):
            table_path = os.path.join(GLOBAL_STATE.tpcds_data_path, item)
            if os.path.isdir(table_path):
                # Check if directory contains CSV files
                csv_files = glob.glob(os.path.join(table_path, "*.csv"))
                if csv_files:
                    # Use absolute path for the table
                    tables[item] = os.path.abspath(table_path)
                    print(f"Found TPCDS table: {item} -> {tables[item]}")

                    # Check if we have schema for this table
                    if item not in GLOBAL_STATE.table_schemas:
                        print(f"Warning: No schema defined for table '{item}'. Skipping.")
                        del tables[item]

        return tables

    def setup_flink_environment(self):
        global GLOBAL_STATE
        """Setup the Flink environment once with all TPCDS CSV tables"""
        print(f"[{datetime.now()}] Setting up Flink environment...")
        print(f"[{datetime.now()}] Python executable: {sys.executable}")
        print(f"[{datetime.now()}] TPCDS data path: {GLOBAL_STATE.tpcds_data_path}")

        # Discover tables
        tables = self.discover_tpcds_tables()
        print(f"[{datetime.now()}] Discovered {len(tables)} TPCDS tables: {list(tables.keys())}")

        # Create the Flink environment
        try:
            from pyflink.table import EnvironmentSettings, TableEnvironment

            # Create environment
            env_settings = EnvironmentSettings.in_batch_mode()
            table_env = TableEnvironment.create(env_settings)

            # Register all tables with their schemas
            for table_name, table_path in tables.items():
                print(f"[{datetime.now()}] Registering table: {table_name}")

                # Get schema for this table
                schema_def = ', '.join(GLOBAL_STATE.table_schemas[table_name])

                # Create table with explicit schema
                create_table_sql = f"""
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

                table_env.execute_sql(create_table_sql)

            print(f"[{datetime.now()}] Verifying TPCDS tables...")
            for table_name in tables.keys():
                try:
                    print(f"[{datetime.now()}] Testing table: {table_name}")
                    result = table_env.sql_query(f"SELECT COUNT(*) as row_count FROM {table_name}")
                    count_result = result.execute().collect()
                    row_count = list(count_result)[0][0]
                    print(f"[{datetime.now()}]   {table_name}: {row_count} rows")

                    # Show schema
                    schema_result = table_env.sql_query(f"SELECT * FROM {table_name} LIMIT 1")
                    schema_names = schema_result.get_schema().get_field_names()
                    print(f"[{datetime.now()}]   {table_name} columns: {schema_names[:5]}{'...' if len(schema_names) > 5 else ''}")

                except Exception as e:
                    print(f"[{datetime.now()}]   ERROR with {table_name}: {e}")

            print(f"[{datetime.now()}] Table verification complete.")

            # Create namespace with Flink objects
            namespace = {
                'table_env': table_env,
                'EnvironmentSettings': EnvironmentSettings,
                'TableEnvironment': TableEnvironment,
            }

            print(f"[{datetime.now()}] Flink environment setup successful!")
            return namespace

        except Exception as e:
            print(f"[{datetime.now()}] Failed to setup Flink environment: {str(e)}")
            print(f"[{datetime.now()}] {traceback.format_exc()}")
            return None

    def setup_flink_namespace(self):
        global GLOBAL_STATE
        """Return a copy of the pre-initialized Flink namespace"""
        if GLOBAL_STATE.flink_namespace is None:
            return None, "Flink environment was not initialized properly"

        # Return a copy of the namespace for this execution
        return dict(GLOBAL_STATE.flink_namespace), None


    def add_imports(self, received_code):
        return f"""
from pyflink.table.expressions import col, lit
from pyflink.table import expressions as expr

{received_code}
"""

    def execute_flink_code(self, received_code):
        """Execute the received code in the pre-initialized Flink namespace"""
        stdout_capture = StringIO()
        stderr_capture = StringIO()

        full_code = received_code
#         full_code = self.add_imports(received_code)
        try:
            # Get the pre-initialized namespace
            namespace, setup_error = self.setup_flink_namespace()

            if setup_error:
                print(f"[{datetime.now()}] Setup error occurred:")
                print(setup_error)
                return {
                    'stdout': '',
                    'stderr': setup_error,
                    'return_code': -1,
                    'success': False
                }

            # Execute the received code in the namespace with output capture
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                try:
                    print(f"[{datetime.now()}] Executing received code...")
                    # Try to execute as statements first
                    exec(full_code, namespace, namespace)
                    success = True
                    error_msg = None
                    print(f"[{datetime.now()}] Code executed successfully via exec()")
                except SyntaxError as se:
                    print(f"[{datetime.now()}] exec() failed with SyntaxError, trying eval()...")
                    # If exec fails, try eval for expressions
                    try:
                        result = eval(full_code, namespace, namespace)
                        if result is not None:
                            print(result)  # This will go to stdout_capture
                        success = True
                        error_msg = None
                        print(f"[{datetime.now()}] Code executed successfully via eval()")
                    except Exception as e:
                        success = False
                        error_msg = f"Eval execution error: {str(e)}\n{traceback.format_exc()}"
                        print(f"[{datetime.now()}] Eval failed: {error_msg}")
                except Exception as e:
                    success = False
                    error_msg = f"Exec execution error: {str(e)}\n{traceback.format_exc()}"
                    print(f"[{datetime.now()}] Exec failed: {error_msg}")

            stdout_output = stdout_capture.getvalue()
            stderr_output = stderr_capture.getvalue()

            # Add any execution error to stderr
            if error_msg:
                stderr_output += error_msg

            print(f"[{datetime.now()}] Code execution completed. Success: {success}")

            if stdout_output:
                print(f"[{datetime.now()}] STDOUT:")
                print("-" * 40)
                print(stdout_output)
                print("-" * 40)
            else:
                print(f"[{datetime.now()}] No stdout output")

            if stderr_output:
                print(f"[{datetime.now()}] STDERR:")
                print("-" * 40)
                print(stderr_output)
                print("-" * 40)
            else:
                print(f"[{datetime.now()}] No stderr output")

            return {
                'stdout': stdout_output,
                'stderr': stderr_output,
                'return_code': 0 if success else 1,
                'success': success
            }

        except Exception as e:
            error_msg = f"Critical error during execution: {str(e)}\n{traceback.format_exc()}"
            print(f"[{datetime.now()}] CRITICAL ERROR:")
            print("-" * 50)
            print(error_msg)
            print("-" * 50)
            return {
                'stdout': stdout_capture.getvalue(),
                'stderr': error_msg,
                'return_code': -1,
                'success': False,
                'namespace': None
            }

    def handle_execute_code(self, request_dict):
        global GLOBAL_STATE
        """Handle individual client connections"""
        print(f"[{datetime.now()}] Running handle_execute_code()...")
        data = request_dict['code']
        response_dict = {}
        try:
            program_number = self.get_next_program_number()

            print(f"\n{'='*60}")
            print(f"PROGRAM #{program_number} RECEIVED")
            print(f"Length: {len(data)} characters")
            print(f"{'='*60}")
            print(data)
            print(f"{'='*60}")

            # Execute the received code with the Flink template
            print(f"[{datetime.now()}] Executing program #{program_number} with Flink template...")
            execution_result = self.execute_flink_code(data)

            # Store results with namespace access
            stdout_output = execution_result['stdout']
            stderr_output = execution_result['stderr']
            return_code = execution_result['return_code']
            success = execution_result['success']
            namespace = execution_result.get('namespace', {})

            # Save logs to disk
            self.save_program_logs(program_number, data, stdout_output, stderr_output)

            # You can now access Flink objects from the namespace if needed
            # For example: table_env = namespace.get('table_env')

            print(f"\n{'='*60}")
            print(f"EXECUTION RESULTS FOR PROGRAM #{program_number}")
            print(f"Success: {success}")
            print(f"Return Code: {return_code}")
            print(f"Logs saved to: {os.path.join(GLOBAL_STATE.log_dir, str(program_number))}")
            if stderr_output:
                print(f"Error Details:")
                print("-" * 30)
                print(stderr_output)
                print("-" * 30)
            print(f"Available variables in namespace: {list(namespace.keys()) if namespace else 'None'}")
            print(f"{'='*60}\n")
            response_dict = execution_result
        except Exception as e:
            print(f"[{datetime.now()}] Error handling client {e}")

        return response_dict

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
        def parse_data_type(type_str):
            """Parse data type string and return standardized type"""
            type_str = type_str.upper()

            if 'INT' in type_str:
                return 'integer'
            elif 'STRING' in type_str or 'VARCHAR' in type_str or 'CHAR' in type_str:
                return 'string'
            elif 'DECIMAL' in type_str or 'NUMERIC' in type_str or 'FLOAT' in type_str:
                return 'decimal'
            elif 'DATE' in type_str or 'TIMESTAMP' in type_str:
                return 'date'
            elif 'BOOL' in type_str:
                return 'boolean'
            else:
                return 'string'  # Default fallback

        def is_key_column(column_name):
            """Determine if a column is likely a key based on naming patterns"""
            name_lower = column_name.lower()
            # Common key patterns
            key_patterns = ['_id', '_sk', '_key', 'id_', 'key_']
            return any(pattern in name_lower for pattern in key_patterns) or name_lower == 'id'

        def is_nullable_column(column_name, data_type):
            """Determine if a column should be nullable based on patterns"""
            name_lower = column_name.lower()

            # Keys are typically not nullable
            if is_key_column(column_name):
                return False

            # Some fields are commonly required
            required_patterns = ['name', 'type', 'class', 'status']
            if any(pattern in name_lower for pattern in required_patterns):
                return False

            # Optional fields are commonly nullable
            optional_patterns = ['desc', 'description', 'manager', 'suite', 'county']
            if any(pattern in name_lower for pattern in optional_patterns):
                return True

            # Default: most fields can be nullable
            return True

        result = []

        for table_name, columns in input_dict.items():
            table_schema = {
                "identifier": table_name,
                "columns": [],
                "metadata": {
                    "source": "database",
                    "owner": "admin"
                }
            }

            for column_def in columns:
                # Split column definition into name and type
                parts = column_def.split()
                if len(parts) >= 2:
                    column_name = parts[0]
                    data_type_raw = ' '.join(parts[1:])  # Handle types like "DECIMAL(5,2)"

                    column_schema = {
                        "name": column_name,
                        "dataType": parse_data_type(data_type_raw),
                        "isNullable": is_nullable_column(column_name, data_type_raw),
                        "isKey": is_key_column(column_name)
                    }

                    table_schema["columns"].append(column_schema)

            result.append(table_schema)

        return result

    def handle_load_data(self, request_dict: Dict[str, Any]) -> Dict[str, Any]:
        global GLOBAL_STATE
        GLOBAL_STATE.host = 'localhost'
        GLOBAL_STATE.port = 8888
        GLOBAL_STATE.running = True
        GLOBAL_STATE.tpcds_data_path = 'tpcds-csv'
        GLOBAL_STATE.program_counter = 0
        GLOBAL_STATE.counter_lock = threading.Lock()
        GLOBAL_STATE.log_dir = 'server-log'

        self.setup_logging_directory(GLOBAL_STATE.log_dir)

        GLOBAL_STATE.table_schemas = self.load_tpcds_schema("pyflink-oracle-server/tpcds-schema.json")
        # Initialize Flink environment once
        GLOBAL_STATE.flink_namespace = self.setup_flink_environment()

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

if __name__ == '__main__':
    run_server()


"""
[
                {
                    "identifier": "users",
                    "columns": [
                        {
                            "name": "id",
                            "dataType": "integer",
                            "isNullable": False,
                            "isKey": True
                        },
                        {
                            "name": "username",
                            "dataType": "string",
                            "isNullable": False,
                            "isKey": False
                        },
                        {
                            "name": "email",
                            "dataType": "string",
                            "isNullable": True,
                            "isKey": False
                        }
                    ],
                    "metadata": {
                        "source": "database",
                        "owner": "admin"
                    }
                },
                {
                    "identifier": "products",
                    "columns": [
                        {
                            "name": "id",
                            "dataType": "integer",
                            "isNullable": False,
                            "isKey": True
                        },
                        {
                            "name": "name",
                            "dataType": "string",
                            "isNullable": False
                        },
                        {
                            "name": "price",
                            "dataType": "decimal",
                            "isNullable": True,
                            "defaultValue": "0.00"
                        }
                    ],
                    "metadata": {}
                }
            ]
"""