#!/usr/bin/env python3

import socket
import threading
import sys
from datetime import datetime
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
import traceback
import os
import glob

class FlinkSourceCodeServer:
    def __init__(self, host='localhost', port=8888, tpcds_data_path='tpcds-data'):
        self.host = host
        self.port = port
        self.running = True
        self.server_socket = None
        self.tpcds_data_path = tpcds_data_path
        self.program_counter = 0
        self.counter_lock = threading.Lock()
        self.log_dir = 'server-log'
        self.setup_logging_directory()

        self.table_schemas = {
                            'call_center': [
                                'cc_call_center_sk INT',
                                'cc_call_center_id STRING',
                                'cc_rec_start_date STRING',
                                'cc_rec_end_date STRING',
                                'cc_closed_date_sk INT',
                                'cc_open_date_sk INT',
                                'cc_name STRING',
                                'cc_class STRING',
                                'cc_employees INT',
                                'cc_sq_ft INT',
                                'cc_hours STRING',
                                'cc_manager STRING',
                                'cc_mkt_id INT',
                                'cc_mkt_class STRING',
                                'cc_mkt_desc STRING',
                                'cc_market_manager STRING',
                                'cc_division INT',
                                'cc_division_name STRING',
                                'cc_company INT',
                                'cc_company_name STRING',
                                'cc_street_number STRING',
                                'cc_street_name STRING',
                                'cc_street_type STRING',
                                'cc_suite_number STRING',
                                'cc_city STRING',
                                'cc_county STRING',
                                'cc_state STRING',
                                'cc_zip STRING',
                                'cc_country STRING',
                                'cc_gmt_offset DECIMAL(5,2)',
                                'cc_tax_percentage DECIMAL(5,2)'
                            ],
                            'catalog_page': [
                                'cp_catalog_page_sk INT',
                                'cp_catalog_page_id STRING',
                                'cp_start_date_sk INT',
                                'cp_end_date_sk INT',
                                'cp_department STRING',
                                'cp_catalog_number INT',
                                'cp_catalog_page_number INT',
                                'cp_description STRING',
                                'cp_type STRING'
                            ],
                            'catalog_returns': [
                                'cr_returned_date_sk INT',
                                'cr_returned_time_sk INT',
                                'cr_item_sk INT',
                                'cr_refunded_customer_sk INT',
                                'cr_refunded_cdemo_sk INT',
                                'cr_refunded_hdemo_sk INT',
                                'cr_refunded_addr_sk INT',
                                'cr_returning_customer_sk INT',
                                'cr_returning_cdemo_sk INT',
                                'cr_returning_hdemo_sk INT',
                                'cr_returning_addr_sk INT',
                                'cr_call_center_sk INT',
                                'cr_catalog_page_sk INT',
                                'cr_ship_mode_sk INT',
                                'cr_warehouse_sk INT',
                                'cr_reason_sk INT',
                                'cr_order_number INT',
                                'cr_return_quantity INT',
                                'cr_return_amount DECIMAL(7,2)',
                                'cr_return_tax DECIMAL(7,2)',
                                'cr_return_amt_inc_tax DECIMAL(7,2)',
                                'cr_fee DECIMAL(7,2)',
                                'cr_return_ship_cost DECIMAL(7,2)',
                                'cr_refunded_cash DECIMAL(7,2)',
                                'cr_reversed_charge DECIMAL(7,2)',
                                'cr_store_credit DECIMAL(7,2)',
                                'cr_net_loss DECIMAL(7,2)'
                            ],
                            'catalog_sales': [
                                'cs_sold_date_sk INT',
                                'cs_sold_time_sk INT',
                                'cs_ship_date_sk INT',
                                'cs_bill_customer_sk INT',
                                'cs_bill_cdemo_sk INT',
                                'cs_bill_hdemo_sk INT',
                                'cs_bill_addr_sk INT',
                                'cs_ship_customer_sk INT',
                                'cs_ship_cdemo_sk INT',
                                'cs_ship_hdemo_sk INT',
                                'cs_ship_addr_sk INT',
                                'cs_call_center_sk INT',
                                'cs_catalog_page_sk INT',
                                'cs_ship_mode_sk INT',
                                'cs_warehouse_sk INT',
                                'cs_item_sk INT',
                                'cs_promo_sk INT',
                                'cs_order_number INT',
                                'cs_quantity INT',
                                'cs_wholesale_cost DECIMAL(7,2)',
                                'cs_list_price DECIMAL(7,2)',
                                'cs_sales_price DECIMAL(7,2)',
                                'cs_ext_discount_amt DECIMAL(7,2)',
                                'cs_ext_sales_price DECIMAL(7,2)',
                                'cs_ext_wholesale_cost DECIMAL(7,2)',
                                'cs_ext_list_price DECIMAL(7,2)',
                                'cs_ext_tax DECIMAL(7,2)',
                                'cs_coupon_amt DECIMAL(7,2)',
                                'cs_ext_ship_cost DECIMAL(7,2)',
                                'cs_net_paid DECIMAL(7,2)',
                                'cs_net_paid_inc_tax DECIMAL(7,2)',
                                'cs_net_paid_inc_ship DECIMAL(7,2)',
                                'cs_net_paid_inc_ship_tax DECIMAL(7,2)',
                                'cs_net_profit DECIMAL(7,2)'
                            ],
                            'customer': [
                                'c_customer_sk INT',
                                'c_customer_id STRING',
                                'c_current_cdemo_sk INT',
                                'c_current_hdemo_sk INT',
                                'c_current_addr_sk INT',
                                'c_first_shipto_date_sk INT',
                                'c_first_sales_date_sk INT',
                                'c_salutation STRING',
                                'c_first_name STRING',
                                'c_last_name STRING',
                                'c_preferred_cust_flag STRING',
                                'c_birth_day INT',
                                'c_birth_month INT',
                                'c_birth_year INT',
                                'c_birth_country STRING',
                                'c_login STRING',
                                'c_email_address STRING',
                                'c_last_review_date STRING'
                            ],
                            'customer_address': [
                                'ca_address_sk INT',
                                'ca_address_id STRING',
                                'ca_street_number STRING',
                                'ca_street_name STRING',
                                'ca_street_type STRING',
                                'ca_suite_number STRING',
                                'ca_city STRING',
                                'ca_county STRING',
                                'ca_state STRING',
                                'ca_zip STRING',
                                'ca_country STRING',
                                'ca_gmt_offset DECIMAL(5,2)',
                                'ca_location_type STRING'
                            ],
                            'customer_demographics': [
                                'cd_demo_sk INT',
                                'cd_gender STRING',
                                'cd_marital_status STRING',
                                'cd_education_status STRING',
                                'cd_purchase_estimate INT',
                                'cd_credit_rating STRING',
                                'cd_dep_count INT',
                                'cd_dep_employed_count INT',
                                'cd_dep_college_count INT'
                            ],
                            'date_dim': [
                                'd_date_sk INT',
                                'd_date_id STRING',
                                'd_date STRING',
                                'd_month_seq INT',
                                'd_week_seq INT',
                                'd_quarter_seq INT',
                                'd_year INT',
                                'd_dow INT',
                                'd_moy INT',
                                'd_dom INT',
                                'd_qoy INT',
                                'd_fy_year INT',
                                'd_fy_quarter_seq INT',
                                'd_fy_week_seq INT',
                                'd_day_name STRING',
                                'd_quarter_name STRING',
                                'd_holiday STRING',
                                'd_weekend STRING',
                                'd_following_holiday STRING',
                                'd_first_dom INT',
                                'd_last_dom INT',
                                'd_same_day_ly INT',
                                'd_same_day_lq INT',
                                'd_current_day STRING',
                                'd_current_week STRING',
                                'd_current_month STRING',
                                'd_current_quarter STRING',
                                'd_current_year STRING'
                            ],
                            'household_demographics': [
                                'hd_demo_sk INT',
                                'hd_income_band_sk INT',
                                'hd_buy_potential STRING',
                                'hd_dep_count INT',
                                'hd_vehicle_count INT'
                            ],
                            'income_band': [
                                'ib_income_band_sk INT',
                                'ib_lower_bound INT',
                                'ib_upper_bound INT'
                            ],
                            'inventory': [
                                'inv_date_sk INT',
                                'inv_item_sk INT',
                                'inv_warehouse_sk INT',
                                'inv_quantity_on_hand INT'
                            ],
                            'item': [
                                'i_item_sk INT',
                                'i_item_id STRING',
                                'i_rec_start_date STRING',
                                'i_rec_end_date STRING',
                                'i_item_desc STRING',
                                'i_current_price DECIMAL(7,2)',
                                'i_wholesale_cost DECIMAL(7,2)',
                                'i_brand_id INT',
                                'i_brand STRING',
                                'i_class_id INT',
                                'i_class STRING',
                                'i_category_id INT',
                                'i_category STRING',
                                'i_manufact_id INT',
                                'i_manufact STRING',
                                'i_size STRING',
                                'i_formulation STRING',
                                'i_color STRING',
                                'i_units STRING',
                                'i_container STRING',
                                'i_manager_id INT',
                                'i_product_name STRING'
                            ],
                            'promotion': [
                                'p_promo_sk INT',
                                'p_promo_id STRING',
                                'p_start_date_sk INT',
                                'p_end_date_sk INT',
                                'p_item_sk INT',
                                'p_cost DECIMAL(15,2)',
                                'p_response_target INT',
                                'p_promo_name STRING',
                                'p_channel_dmail STRING',
                                'p_channel_email STRING',
                                'p_channel_catalog STRING',
                                'p_channel_tv STRING',
                                'p_channel_radio STRING',
                                'p_channel_press STRING',
                                'p_channel_event STRING',
                                'p_channel_demo STRING',
                                'p_channel_details STRING',
                                'p_purpose STRING',
                                'p_discount_active STRING'
                            ],
                            'reason': [
                                'r_reason_sk INT',
                                'r_reason_id STRING',
                                'r_reason_desc STRING'
                            ],
                            'ship_mode': [
                                'sm_ship_mode_sk INT',
                                'sm_ship_mode_id STRING',
                                'sm_type STRING',
                                'sm_code STRING',
                                'sm_carrier STRING',
                                'sm_contract STRING'
                            ],
                            'store': [
                                's_store_sk INT',
                                's_store_id STRING',
                                's_rec_start_date STRING',
                                's_rec_end_date STRING',
                                's_closed_date_sk INT',
                                's_store_name STRING',
                                's_number_employees INT',
                                's_floor_space INT',
                                's_hours STRING',
                                's_manager STRING',
                                's_market_id INT',
                                's_geography_class STRING',
                                's_market_desc STRING',
                                's_market_manager STRING',
                                's_division_id INT',
                                's_division_name STRING',
                                's_company_id INT',
                                's_company_name STRING',
                                's_street_number STRING',
                                's_street_name STRING',
                                's_street_type STRING',
                                's_suite_number STRING',
                                's_city STRING',
                                's_county STRING',
                                's_state STRING',
                                's_zip STRING',
                                's_country STRING',
                                's_gmt_offset DECIMAL(5,2)',
                                's_tax_precentage DECIMAL(5,2)'
                            ],
                            'store_returns': [
                                'sr_returned_date_sk INT',
                                'sr_return_time_sk INT',
                                'sr_item_sk INT',
                                'sr_customer_sk INT',
                                'sr_cdemo_sk INT',
                                'sr_hdemo_sk INT',
                                'sr_addr_sk INT',
                                'sr_store_sk INT',
                                'sr_reason_sk INT',
                                'sr_ticket_number INT',
                                'sr_return_quantity INT',
                                'sr_return_amt DECIMAL(7,2)',
                                'sr_return_tax DECIMAL(7,2)',
                                'sr_return_amt_inc_tax DECIMAL(7,2)',
                                'sr_fee DECIMAL(7,2)',
                                'sr_return_ship_cost DECIMAL(7,2)',
                                'sr_refunded_cash DECIMAL(7,2)',
                                'sr_reversed_charge DECIMAL(7,2)',
                                'sr_store_credit DECIMAL(7,2)',
                                'sr_net_loss DECIMAL(7,2)'
                            ],
                            'store_sales': [
                                'ss_sold_date_sk INT',
                                'ss_sold_time_sk INT',
                                'ss_item_sk INT',
                                'ss_customer_sk INT',
                                'ss_cdemo_sk INT',
                                'ss_hdemo_sk INT',
                                'ss_addr_sk INT',
                                'ss_store_sk INT',
                                'ss_promo_sk INT',
                                'ss_ticket_number INT',
                                'ss_quantity INT',
                                'ss_wholesale_cost DECIMAL(7,2)',
                                'ss_list_price DECIMAL(7,2)',
                                'ss_sales_price DECIMAL(7,2)',
                                'ss_ext_discount_amt DECIMAL(7,2)',
                                'ss_ext_sales_price DECIMAL(7,2)',
                                'ss_ext_wholesale_cost DECIMAL(7,2)',
                                'ss_ext_list_price DECIMAL(7,2)',
                                'ss_ext_tax DECIMAL(7,2)',
                                'ss_coupon_amt DECIMAL(7,2)',
                                'ss_net_paid DECIMAL(7,2)',
                                'ss_net_paid_inc_tax DECIMAL(7,2)',
                                'ss_net_profit DECIMAL(7,2)'
                            ],
                            'time_dim': [
                                't_time_sk INT',
                                't_time_id STRING',
                                't_time INT',
                                't_hour INT',
                                't_minute INT',
                                't_second INT',
                                't_am_pm STRING',
                                't_shift STRING',
                                't_sub_shift STRING',
                                't_meal_time STRING'
                            ],
                            'warehouse': [
                                'w_warehouse_sk INT',
                                'w_warehouse_id STRING',
                                'w_warehouse_name STRING',
                                'w_warehouse_sq_ft INT',
                                'w_street_number STRING',
                                'w_street_name STRING',
                                'w_street_type STRING',
                                'w_suite_number STRING',
                                'w_city STRING',
                                'w_county STRING',
                                'w_state STRING',
                                'w_zip STRING',
                                'w_country STRING',
                                'w_gmt_offset DECIMAL(5,2)'
                            ],
                            'web_page': [
                                'wp_web_page_sk INT',
                                'wp_web_page_id STRING',
                                'wp_rec_start_date STRING',
                                'wp_rec_end_date STRING',
                                'wp_creation_date_sk INT',
                                'wp_access_date_sk INT',
                                'wp_autogen_flag STRING',
                                'wp_customer_sk INT',
                                'wp_url STRING',
                                'wp_type STRING',
                                'wp_char_count INT',
                                'wp_link_count INT',
                                'wp_image_count INT',
                                'wp_max_ad_count INT'
                            ],
                            'web_returns': [
                                'wr_returned_date_sk INT',
                                'wr_returned_time_sk INT',
                                'wr_item_sk INT',
                                'wr_refunded_customer_sk INT',
                                'wr_refunded_cdemo_sk INT',
                                'wr_refunded_hdemo_sk INT',
                                'wr_refunded_addr_sk INT',
                                'wr_returning_customer_sk INT',
                                'wr_returning_cdemo_sk INT',
                                'wr_returning_hdemo_sk INT',
                                'wr_returning_addr_sk INT',
                                'wr_web_page_sk INT',
                                'wr_reason_sk INT',
                                'wr_order_number INT',
                                'wr_return_quantity INT',
                                'wr_return_amt DECIMAL(7,2)',
                                'wr_return_tax DECIMAL(7,2)',
                                'wr_return_amt_inc_tax DECIMAL(7,2)',
                                'wr_fee DECIMAL(7,2)',
                                'wr_return_ship_cost DECIMAL(7,2)',
                                'wr_refunded_cash DECIMAL(7,2)',
                                'wr_reversed_charge DECIMAL(7,2)',
                                'wr_account_credit DECIMAL(7,2)',
                                'wr_net_loss DECIMAL(7,2)'
                            ],
                            'web_sales': [
                                'ws_sold_date_sk INT',
                                'ws_sold_time_sk INT',
                                'ws_ship_date_sk INT',
                                'ws_item_sk INT',
                                'ws_bill_customer_sk INT',
                                'ws_bill_cdemo_sk INT',
                                'ws_bill_hdemo_sk INT',
                                'ws_bill_addr_sk INT',
                                'ws_ship_customer_sk INT',
                                'ws_ship_cdemo_sk INT',
                                'ws_ship_hdemo_sk INT',
                                'ws_ship_addr_sk INT',
                                'ws_web_page_sk INT',
                                'ws_web_site_sk INT',
                                'ws_ship_mode_sk INT',
                                'ws_warehouse_sk INT',
                                'ws_promo_sk INT',
                                'ws_order_number INT',
                                'ws_quantity INT',
                                'ws_wholesale_cost DECIMAL(7,2)',
                                'ws_list_price DECIMAL(7,2)',
                                'ws_sales_price DECIMAL(7,2)',
                                'ws_ext_discount_amt DECIMAL(7,2)',
                                'ws_ext_sales_price DECIMAL(7,2)',
                                'ws_ext_wholesale_cost DECIMAL(7,2)',
                                'ws_ext_list_price DECIMAL(7,2)',
                                'ws_ext_tax DECIMAL(7,2)',
                                'ws_coupon_amt DECIMAL(7,2)',
                                'ws_ext_ship_cost DECIMAL(7,2)',
                                'ws_net_paid DECIMAL(7,2)',
                                'ws_net_paid_inc_tax DECIMAL(7,2)',
                                'ws_net_paid_inc_ship DECIMAL(7,2)',
                                'ws_net_paid_inc_ship_tax DECIMAL(7,2)',
                                'ws_net_profit DECIMAL(7,2)'
                            ],
                            'web_site': [
                                'web_site_sk INT',
                                'web_site_id STRING',
                                'web_rec_start_date STRING',
                                'web_rec_end_date STRING',
                                'web_name STRING',
                                'web_open_date_sk INT',
                                'web_close_date_sk INT',
                                'web_class STRING',
                                'web_manager STRING',
                                'web_mkt_id INT',
                                'web_mkt_class STRING',
                                'web_mkt_desc STRING',
                                'web_market_manager STRING',
                                'web_company_id INT',
                                'web_company_name STRING',
                                'web_street_number STRING',
                                'web_street_name STRING',
                                'web_street_type STRING',
                                'web_suite_number STRING',
                                'web_city STRING',
                                'web_county STRING',
                                'web_state STRING',
                                'web_zip STRING',
                                'web_country STRING',
                                'web_gmt_offset DECIMAL(5,2)',
                                'web_tax_percentage DECIMAL(5,2)'
                            ]
                        }
        # Initialize Flink environment once
        self.flink_namespace = self.setup_flink_environment()

    def setup_logging_directory(self):
        """Create the server-log directory if it doesn't exist"""
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
            print(f"[{datetime.now()}] Created logging directory: {self.log_dir}")
        else:
            print(f"[{datetime.now()}] Using existing logging directory: {self.log_dir}")

    def get_next_program_number(self):
        """Get the next program sequence number (thread-safe)"""
        with self.counter_lock:
            current_number = self.program_counter
            self.program_counter += 1
            return current_number

    def create_program_log_directory(self, program_number):
        """Create a directory for this program's logs"""
        program_dir = os.path.join(self.log_dir, str(program_number))
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

        if not os.path.exists(self.tpcds_data_path):
            print(f"Warning: TPCDS data path '{self.tpcds_data_path}' does not exist")
            return tables

        # Get all subdirectories (table names)
        for item in os.listdir(self.tpcds_data_path):
            table_path = os.path.join(self.tpcds_data_path, item)
            if os.path.isdir(table_path):
                # Check if directory contains CSV files
                csv_files = glob.glob(os.path.join(table_path, "*.csv"))
                if csv_files:
                    # Use absolute path for the table
                    tables[item] = os.path.abspath(table_path)
                    print(f"Found TPCDS table: {item} -> {tables[item]}")

                    # Check if we have schema for this table
                    if item not in self.table_schemas:
                        print(f"Warning: No schema defined for table '{item}'. Skipping.")
                        del tables[item]

        return tables

    def setup_flink_environment(self):
        """Setup the Flink environment once with all TPCDS CSV tables"""
        print(f"[{datetime.now()}] Setting up Flink environment...")
        print(f"[{datetime.now()}] Python executable: {sys.executable}")
        print(f"[{datetime.now()}] TPCDS data path: {self.tpcds_data_path}")

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
                schema_def = ', '.join(self.table_schemas[table_name])

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
        """Return a copy of the pre-initialized Flink namespace"""
        if self.flink_namespace is None:
            return None, "Flink environment was not initialized properly"

        # Return a copy of the namespace for this execution
        return dict(self.flink_namespace), None


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
                'success': success,
                'namespace': namespace  # Include namespace for potential inspection
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
    def receive_complete_data(self, client_socket):
        """Receive complete data from client socket"""
        try:
            # Set a timeout to avoid hanging indefinitely
            client_socket.settimeout(10.0)

            # First, try to read the length (if sent)
            first_line = b""
            while b"\n" not in first_line:
                chunk = client_socket.recv(1)
                if not chunk:
                    # No length prefix, fall back to read-until-close
                    return self.receive_data_until_close(client_socket, first_line)
                first_line += chunk

            first_line = first_line.decode('utf-8').strip()

            # Check if first line is a number (length prefix)
            if first_line.isdigit():
                expected_length = int(first_line)
                print(f"[{datetime.now()}] Expecting {expected_length} characters")

                # Read exactly that many bytes
                data_chunks = []
                total_received = 0

                while total_received < expected_length:
                    remaining = expected_length - total_received
                    chunk_size = min(4096, remaining)
                    chunk = client_socket.recv(chunk_size)

                    if not chunk:
                        break

                    data_chunks.append(chunk)
                    total_received += len(chunk)
                    print(f"[{datetime.now()}] Received {len(chunk)} bytes, total: {total_received}/{expected_length}")

                # Read the trailing newline
                client_socket.recv(1)  # consume the \n

                complete_data = b''.join(data_chunks).decode('utf-8')
                print(f"[{datetime.now()}] Received complete data: {len(complete_data)} characters")
                return complete_data

            else:
                # First line is not a number, treat as old protocol or KILL command
                if first_line == "KILL":
                    return "KILL"
                # Fall back to read until close, including the first line
                return self.receive_data_until_close(client_socket, first_line.encode('utf-8') + b'\n')

        except Exception as e:
            print(f"[{datetime.now()}] Error receiving data: {e}")
            return None

    def receive_data_until_close(self, client_socket, initial_data=b""):
        """Fallback method: read until socket closes"""
        data_chunks = [initial_data] if initial_data else []

        try:
            while True:
                try:
                    chunk = client_socket.recv(4096)
                    if not chunk:
                        break
                    data_chunks.append(chunk)
                    print(f"[{datetime.now()}] Received chunk of {len(chunk)} bytes")
                except socket.timeout:
                    print(f"[{datetime.now()}] Socket timeout - assuming all data received")
                    break

            complete_data = b''.join(data_chunks).decode('utf-8')
            if complete_data.endswith('\n'):
                complete_data = complete_data.rstrip('\n')

            print(f"[{datetime.now()}] Total data received: {len(complete_data)} characters")
            return complete_data

        except Exception as e:
            print(f"[{datetime.now()}] Error in fallback receive: {e}")
            return None

    def handle_client(self, client_socket, client_address):
        """Handle individual client connections"""
        print(f"[{datetime.now()}] New connection from {client_address}")

        try:
            # Receive complete data from client
            data = self.receive_complete_data(client_socket)

            if not data:
                print(f"[{datetime.now()}] No data received from {client_address}")
                return

            # Check for kill command
            if data.strip() == "KILL":
                print(f"[{datetime.now()}] Kill command received from {client_address}")
                print("Shutting down server...")
                self.running = False
                return

            print(f"[{datetime.now()}] Received {len(data)} characters of data")

            # Get program number for this execution
            program_number = self.get_next_program_number()

            # Print received source code
            print(f"\n{'='*60}")
            print(f"PROGRAM #{program_number} RECEIVED FROM {client_address}")
            print(f"Timestamp: {datetime.now()}")
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
            print(f"Logs saved to: {os.path.join(self.log_dir, str(program_number))}")
            if stderr_output:
                print(f"Error Details:")
                print("-" * 30)
                print(stderr_output)
                print("-" * 30)
            print(f"Available variables in namespace: {list(namespace.keys()) if namespace else 'None'}")
            print(f"{'='*60}\n")

        except Exception as e:
            print(f"[{datetime.now()}] Error handling client {client_address}: {e}")
        finally:
            client_socket.close()
            print(f"[{datetime.now()}] Connection closed for {client_address}")

    def start_server(self):
        """Start the server and listen for connections"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)

            print(f"Flink Python Source Code Server started on {self.host}:{self.port}")
            print("Waiting for source code... (Send 'KILL' to stop the server)")
            print("-" * 60)

            while self.running:
                try:
                    # Accept client connections
                    client_socket, client_address = self.server_socket.accept()

                    # Handle each client in a separate thread
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, client_address)
                    )
                    client_thread.daemon = True
                    client_thread.start()

                except socket.error as e:
                    if self.running:
                        print(f"Socket error: {e}")
                    break

        except KeyboardInterrupt:
            print("\nKeyboard interrupt received. Shutting down...")
        except Exception as e:
            print(f"Server error: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up server resources"""
        if self.server_socket:
            self.server_socket.close()
        print("Server stopped.")

def main():
    # You can customize host, port, and TPCDS data path here
    host = 'localhost'
    port = 8888
    tpcds_data_path = 'tpcds-csv'

    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    if len(sys.argv) > 2:
        host = sys.argv[2]
    if len(sys.argv) > 3:
        tpcds_data_path = sys.argv[3]

    server = FlinkSourceCodeServer(host, port, tpcds_data_path)
    server.start_server()

if __name__ == "__main__":
    main()