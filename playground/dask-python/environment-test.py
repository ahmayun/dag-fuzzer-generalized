import json
import dask.dataframe as dd

# Load schema
with open("pyflink-oracle-server/tpcds-schema.json") as f:
    schema = json.load(f)

def sql_to_dtype(sql_type: str):
    if sql_type.startswith("INT"):
        return "Int64"  # nullable integer
    if sql_type.startswith("DECIMAL"):
        return "float64"
    if sql_type == "STRING":
        return "object"
    return "object"

# Build dtype map for customer
cust_dtypes = {
    col.split()[0]: sql_to_dtype(col.split()[1])
    for col in schema["customer"]
}

# Same for store_sales
sales_dtypes = {
    col.split()[0]: sql_to_dtype(col.split()[1])
    for col in schema["store_sales"]
}

customer = dd.read_csv("tpcds-csv/customer/*.csv", dtype=cust_dtypes)
store_sales = dd.read_csv("tpcds-csv/store_sales/*.csv", dtype=sales_dtypes)
joined = store_sales.merge(
    customer,
    left_on="ss_customer_sk",
    right_on="c_customer_sk",
    how="inner"
)

result = joined.groupby("c_birth_country")["ss_net_paid"].sum().compute()
print(result.visualize())
