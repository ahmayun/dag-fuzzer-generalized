import json
import dask.dataframe as dd


def get_dtype_dict(table_name, table_schemas):
    """Convert schema definition to pandas dtype dictionary"""
    dtype_dict = {}

    for column_def in table_schemas[table_name]:
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


def load_tpcds_schema(json_file):
    with open(json_file, "r") as f:
        data = json.load(f)
    return data

SCHEMAS = load_tpcds_schema("pyflink-oracle-server/tpcds-schema.json")

def load_dask_table(table_name):
    return dd.read_csv(f'tpcds-csv/{table_name}/*.csv', dtype=get_dtype_dict(table_name, SCHEMAS), assume_missing=True)