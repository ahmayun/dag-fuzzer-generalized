from pyflink.table import EnvironmentSettings, TableEnvironment
import os

def load_tpcds_csv_table(folder_path, table_name="tpcds_table", sample_size=10):
    """
    Load TPC-DS CSV files from a folder and print sample rows

    Args:
        folder_path (str): Path to folder containing CSV files
        table_name (str): Name to assign to the table
        sample_size (int): Number of sample rows to display
    """

    # Validate folder exists and has files
    if not os.path.exists(folder_path):
        print(f"Error: Folder {folder_path} does not exist")
        return

    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if not csv_files:
        print(f"Error: No CSV files found in {folder_path}")
        print(f"Files in folder: {os.listdir(folder_path)}")
        return

    print(f"Found CSV files: {csv_files}")

    # Create batch table environment
    env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    table_env = TableEnvironment.create(env_settings)

    try:
        # Create table pointing to CSV folder - try with more flexible options
        create_table_sql = f"""
        CREATE TABLE {table_name} (
            c_customer_sk INT,
            c_customer_id STRING,
            c_current_cdemo_sk INT,
            c_current_hdemo_sk INT,
            c_current_addr_sk INT,
            c_first_shipto_date_sk INT,
            c_first_sales_date_sk INT,
            c_salutation STRING,
            c_first_name STRING,
            c_last_name STRING,
            c_preferred_cust_flag STRING,
            c_birth_day INT,
            c_birth_month INT,
            c_birth_year INT,
            c_birth_country STRING,
            c_login STRING,
            c_email_address STRING,
            c_last_review_date STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{os.path.abspath(folder_path)}',
            'format' = 'csv',
            'csv.field-delimiter' = ',',
            'csv.ignore-first-line' = 'true',
            'csv.allow-comments' = 'false',
            'csv.ignore-parse-errors' = 'true'
        )
        """

        print(f"Using absolute path: {os.path.abspath(folder_path)}")
        table_env.execute_sql(create_table_sql)
        table = table_env.from_path(table_name)

        # Print table info
        print(f"Loaded table: {table_name}")
        print(f"Schema: {table.get_schema()}")
        print("-" * 60)

        # Try to get table count first
        try:
            count_result = table_env.execute_sql(f"SELECT COUNT(*) FROM {table_name}").collect()
            total_rows = list(count_result)[0][0]
            print(f"Total rows in table: {total_rows}")
        except Exception as count_error:
            print(f"Could not get row count: {count_error}")

        # Get and print sample rows
        print(f"Sample {sample_size} rows:")
        sample_results = table.limit(sample_size).execute().collect()

        row_count = 0
        for i, row in enumerate(sample_results, 1):
            print(f"Row {i}: {row}")
            row_count += 1

        print("-" * 60)
        print(f"Displayed {row_count} rows")

    except Exception as e:
        print(f"Error loading CSV files from {folder_path}: {str(e)}")
        print(f"Full error: {repr(e)}")

def debug_csv_file(file_path, num_lines=5):
    """
    Debug helper to inspect the actual CSV file content
    """
    try:
        print(f"Inspecting file: {file_path}")
        with open(file_path, 'r') as f:
            for i, line in enumerate(f):
                if i >= num_lines:
                    break
                print(f"Line {i+1}: {repr(line)}")
        print("-" * 40)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")

# Example usage
if __name__ == "__main__":
    # Update this path to your TPC-DS CSV folder
    csv_folder_path = "tpcds-csv/customer/"

    # First, let's debug the CSV file content
    csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith('.csv')]
    if csv_files:
        debug_csv_file(os.path.join(csv_folder_path, csv_files[0]))

    load_tpcds_csv_table(csv_folder_path, "customer", 5)