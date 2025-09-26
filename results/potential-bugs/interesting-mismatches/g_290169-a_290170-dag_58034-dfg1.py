# ======== Program ========
from pyflink.table import *
from pyflink.table.expressions import *
from pyflink.table.udf import udf
from pyflink.table.types import DataTypes

from pyflink.table.udf import AggregateFunction, udaf
from pyflink.table import DataTypes
import pandas as pd

class MyObject:
    def __init__(self, name, value):
        self.name = name
        self.value = value

# UDF that returns the custom object
@udf(result_type=DataTypes.ROW([
    DataTypes.FIELD("name", DataTypes.STRING()),
    DataTypes.FIELD("value", DataTypes.INT())
]))
def preloaded_udf_complex(*input_val):
    obj = MyObject("test", hash(input_val[0]))
    return (obj.name, obj.value)  # Return as tuple

@udf(result_type=DataTypes.BOOLEAN())
def preloaded_udf_boolean(input_val):
    return True


def preloaded_aggregation(values: pd.Series) -> float:
    return values.quantile(0.25)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_12 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_11 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_7 = autonode_10.order_by(col('wr_returned_time_sk_node_10'))
autonode_9 = autonode_12.filter(col('c_customer_sk_node_12') <= -9)
autonode_8 = autonode_11.order_by(col('sr_return_amt_inc_tax_node_11'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_6 = autonode_8.join(autonode_9, col('sr_return_time_sk_node_11') == col('c_first_shipto_date_sk_node_12'))
autonode_3 = autonode_5.limit(45)
autonode_4 = autonode_6.group_by(col('sr_store_credit_node_11')).select(col('sr_reason_sk_node_11').max.alias('sr_reason_sk_node_11'))
autonode_2 = autonode_3.join(autonode_4, col('sr_reason_sk_node_11') == col('wr_returning_customer_sk_node_10'))
autonode_1 = autonode_2.order_by(col('wr_reason_sk_node_10'))
sink = autonode_1.add_columns(lit("hello"))
print(sink.explain())

# ======== Details ========
"""
{
  "is_same": false,
  "result_name": "MismatchException",
  "result_details": {
    "opt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalProject(wr_returned_date_sk_node_10=[$0], wr_returned_time_sk_node_10=[$1], wr_item_sk_node_10=[$2], wr_refunded_customer_sk_node_10=[$3], wr_refunded_cdemo_sk_node_10=[$4], wr_refunded_hdemo_sk_node_10=[$5], wr_refunded_addr_sk_node_10=[$6], wr_returning_customer_sk_node_10=[$7], wr_returning_cdemo_sk_node_10=[$8], wr_returning_hdemo_sk_node_10=[$9], wr_returning_addr_sk_node_10=[$10], wr_web_page_sk_node_10=[$11], wr_reason_sk_node_10=[$12], wr_order_number_node_10=[$13], wr_return_quantity_node_10=[$14], wr_return_amt_node_10=[$15], wr_return_tax_node_10=[$16], wr_return_amt_inc_tax_node_10=[$17], wr_fee_node_10=[$18], wr_return_ship_cost_node_10=[$19], wr_refunded_cash_node_10=[$20], wr_reversed_charge_node_10=[$21], wr_account_credit_node_10=[$22], wr_net_loss_node_10=[$23], _c24=[$24], sr_reason_sk_node_11=[$25], _c26=[_UTF-16LE'hello'])
+- LogicalSort(sort0=[$12], dir0=[ASC])
   +- LogicalJoin(condition=[=($25, $7)], joinType=[inner])
      :- LogicalProject(wr_returned_date_sk_node_10=[$0], wr_returned_time_sk_node_10=[$1], wr_item_sk_node_10=[$2], wr_refunded_customer_sk_node_10=[$3], wr_refunded_cdemo_sk_node_10=[$4], wr_refunded_hdemo_sk_node_10=[$5], wr_refunded_addr_sk_node_10=[$6], wr_returning_customer_sk_node_10=[$7], wr_returning_cdemo_sk_node_10=[$8], wr_returning_hdemo_sk_node_10=[$9], wr_returning_addr_sk_node_10=[$10], wr_web_page_sk_node_10=[$11], wr_reason_sk_node_10=[$12], wr_order_number_node_10=[$13], wr_return_quantity_node_10=[$14], wr_return_amt_node_10=[$15], wr_return_tax_node_10=[$16], wr_return_amt_inc_tax_node_10=[$17], wr_fee_node_10=[$18], wr_return_ship_cost_node_10=[$19], wr_refunded_cash_node_10=[$20], wr_reversed_charge_node_10=[$21], wr_account_credit_node_10=[$22], wr_net_loss_node_10=[$23], _c24=[_UTF-16LE'hello'])
      :  +- LogicalSort(sort0=[$1], dir0=[ASC], fetch=[45])
      :     +- LogicalProject(wr_returned_date_sk_node_10=[$0], wr_returned_time_sk_node_10=[$1], wr_item_sk_node_10=[$2], wr_refunded_customer_sk_node_10=[$3], wr_refunded_cdemo_sk_node_10=[$4], wr_refunded_hdemo_sk_node_10=[$5], wr_refunded_addr_sk_node_10=[$6], wr_returning_customer_sk_node_10=[$7], wr_returning_cdemo_sk_node_10=[$8], wr_returning_hdemo_sk_node_10=[$9], wr_returning_addr_sk_node_10=[$10], wr_web_page_sk_node_10=[$11], wr_reason_sk_node_10=[$12], wr_order_number_node_10=[$13], wr_return_quantity_node_10=[$14], wr_return_amt_node_10=[$15], wr_return_tax_node_10=[$16], wr_return_amt_inc_tax_node_10=[$17], wr_fee_node_10=[$18], wr_return_ship_cost_node_10=[$19], wr_refunded_cash_node_10=[$20], wr_reversed_charge_node_10=[$21], wr_account_credit_node_10=[$22], wr_net_loss_node_10=[$23])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
      +- LogicalProject(sr_reason_sk_node_11=[$1])
         +- LogicalAggregate(group=[{18}], EXPR$0=[MAX($8)])
            +- LogicalJoin(condition=[=($1, $25)], joinType=[inner])
               :- LogicalSort(sort0=[$13], dir0=[ASC])
               :  +- LogicalProject(sr_returned_date_sk_node_11=[$0], sr_return_time_sk_node_11=[$1], sr_item_sk_node_11=[$2], sr_customer_sk_node_11=[$3], sr_cdemo_sk_node_11=[$4], sr_hdemo_sk_node_11=[$5], sr_addr_sk_node_11=[$6], sr_store_sk_node_11=[$7], sr_reason_sk_node_11=[$8], sr_ticket_number_node_11=[$9], sr_return_quantity_node_11=[$10], sr_return_amt_node_11=[$11], sr_return_tax_node_11=[$12], sr_return_amt_inc_tax_node_11=[$13], sr_fee_node_11=[$14], sr_return_ship_cost_node_11=[$15], sr_refunded_cash_node_11=[$16], sr_reversed_charge_node_11=[$17], sr_store_credit_node_11=[$18], sr_net_loss_node_11=[$19])
               :     +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
               +- LogicalFilter(condition=[<=($0, -9)])
                  +- LogicalProject(c_customer_sk_node_12=[$0], c_customer_id_node_12=[$1], c_current_cdemo_sk_node_12=[$2], c_current_hdemo_sk_node_12=[$3], c_current_addr_sk_node_12=[$4], c_first_shipto_date_sk_node_12=[$5], c_first_sales_date_sk_node_12=[$6], c_salutation_node_12=[$7], c_first_name_node_12=[$8], c_last_name_node_12=[$9], c_preferred_cust_flag_node_12=[$10], c_birth_day_node_12=[$11], c_birth_month_node_12=[$12], c_birth_year_node_12=[$13], c_birth_country_node_12=[$14], c_login_node_12=[$15], c_email_address_node_12=[$16], c_last_review_date_node_12=[$17])
                     +- LogicalTableScan(table=[[default_catalog, default_database, customer]])

== Optimized Physical Plan ==
Calc(select=[wr_returned_date_sk_node_10, wr_returned_time_sk_node_10, wr_item_sk_node_10, wr_refunded_customer_sk_node_10, wr_refunded_cdemo_sk_node_10, wr_refunded_hdemo_sk_node_10, wr_refunded_addr_sk_node_10, wr_returning_customer_sk_node_10, wr_returning_cdemo_sk_node_10, wr_returning_hdemo_sk_node_10, wr_returning_addr_sk_node_10, wr_web_page_sk_node_10, wr_reason_sk_node_10, wr_order_number_node_10, wr_return_quantity_node_10, wr_return_amt_node_10, wr_return_tax_node_10, wr_return_amt_inc_tax_node_10, wr_fee_node_10, wr_return_ship_cost_node_10, wr_refunded_cash_node_10, wr_reversed_charge_node_10, wr_account_credit_node_10, wr_net_loss_node_10, 'hello' AS _c24, sr_reason_sk_node_11, 'hello' AS _c26])
+- Sort(orderBy=[wr_reason_sk_node_10 ASC])
   +- Exchange(distribution=[single])
      +- HashJoin(joinType=[InnerJoin], where=[=(sr_reason_sk_node_11, wr_returning_customer_sk_node_10)], select=[wr_returned_date_sk_node_10, wr_returned_time_sk_node_10, wr_item_sk_node_10, wr_refunded_customer_sk_node_10, wr_refunded_cdemo_sk_node_10, wr_refunded_hdemo_sk_node_10, wr_refunded_addr_sk_node_10, wr_returning_customer_sk_node_10, wr_returning_cdemo_sk_node_10, wr_returning_hdemo_sk_node_10, wr_returning_addr_sk_node_10, wr_web_page_sk_node_10, wr_reason_sk_node_10, wr_order_number_node_10, wr_return_quantity_node_10, wr_return_amt_node_10, wr_return_tax_node_10, wr_return_amt_inc_tax_node_10, wr_fee_node_10, wr_return_ship_cost_node_10, wr_refunded_cash_node_10, wr_reversed_charge_node_10, wr_account_credit_node_10, wr_net_loss_node_10, _c24, sr_reason_sk_node_11], isBroadcast=[true], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[wr_returned_date_sk AS wr_returned_date_sk_node_10, wr_returned_time_sk AS wr_returned_time_sk_node_10, wr_item_sk AS wr_item_sk_node_10, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_10, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_10, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_10, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_10, wr_returning_customer_sk AS wr_returning_customer_sk_node_10, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_10, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_10, wr_returning_addr_sk AS wr_returning_addr_sk_node_10, wr_web_page_sk AS wr_web_page_sk_node_10, wr_reason_sk AS wr_reason_sk_node_10, wr_order_number AS wr_order_number_node_10, wr_return_quantity AS wr_return_quantity_node_10, wr_return_amt AS wr_return_amt_node_10, wr_return_tax AS wr_return_tax_node_10, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_10, wr_fee AS wr_fee_node_10, wr_return_ship_cost AS wr_return_ship_cost_node_10, wr_refunded_cash AS wr_refunded_cash_node_10, wr_reversed_charge AS wr_reversed_charge_node_10, wr_account_credit AS wr_account_credit_node_10, wr_net_loss AS wr_net_loss_node_10, 'hello' AS _c24])
         :     +- SortLimit(orderBy=[wr_returned_time_sk ASC], offset=[0], fetch=[45], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[wr_returned_time_sk ASC], offset=[0], fetch=[45], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
         +- Calc(select=[EXPR$0 AS sr_reason_sk_node_11])
            +- HashAggregate(isMerge=[true], groupBy=[sr_store_credit], select=[sr_store_credit, Final_MAX(max$0) AS EXPR$0])
               +- Exchange(distribution=[hash[sr_store_credit]])
                  +- LocalHashAggregate(groupBy=[sr_store_credit], select=[sr_store_credit, Partial_MAX(sr_reason_sk) AS max$0])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(sr_return_time_sk, c_first_shipto_date_sk)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[right])
                        :- Sort(orderBy=[sr_return_amt_inc_tax ASC])
                        :  +- Exchange(distribution=[single])
                        :     +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], where=[<=(c_customer_sk, -9)])
                              +- TableSourceScan(table=[[default_catalog, default_database, customer, filter=[<=(c_customer_sk, -9)]]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

== Optimized Execution Plan ==
Calc(select=[wr_returned_date_sk_node_10, wr_returned_time_sk_node_10, wr_item_sk_node_10, wr_refunded_customer_sk_node_10, wr_refunded_cdemo_sk_node_10, wr_refunded_hdemo_sk_node_10, wr_refunded_addr_sk_node_10, wr_returning_customer_sk_node_10, wr_returning_cdemo_sk_node_10, wr_returning_hdemo_sk_node_10, wr_returning_addr_sk_node_10, wr_web_page_sk_node_10, wr_reason_sk_node_10, wr_order_number_node_10, wr_return_quantity_node_10, wr_return_amt_node_10, wr_return_tax_node_10, wr_return_amt_inc_tax_node_10, wr_fee_node_10, wr_return_ship_cost_node_10, wr_refunded_cash_node_10, wr_reversed_charge_node_10, wr_account_credit_node_10, wr_net_loss_node_10, 'hello' AS _c24, sr_reason_sk_node_11, 'hello' AS _c26])
+- Sort(orderBy=[wr_reason_sk_node_10 ASC])
   +- Exchange(distribution=[single])
      +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(sr_reason_sk_node_11 = wr_returning_customer_sk_node_10)], select=[wr_returned_date_sk_node_10, wr_returned_time_sk_node_10, wr_item_sk_node_10, wr_refunded_customer_sk_node_10, wr_refunded_cdemo_sk_node_10, wr_refunded_hdemo_sk_node_10, wr_refunded_addr_sk_node_10, wr_returning_customer_sk_node_10, wr_returning_cdemo_sk_node_10, wr_returning_hdemo_sk_node_10, wr_returning_addr_sk_node_10, wr_web_page_sk_node_10, wr_reason_sk_node_10, wr_order_number_node_10, wr_return_quantity_node_10, wr_return_amt_node_10, wr_return_tax_node_10, wr_return_amt_inc_tax_node_10, wr_fee_node_10, wr_return_ship_cost_node_10, wr_refunded_cash_node_10, wr_reversed_charge_node_10, wr_account_credit_node_10, wr_net_loss_node_10, _c24, sr_reason_sk_node_11], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- Calc(select=[EXPR$0 AS sr_reason_sk_node_11])\
   +- HashAggregate(isMerge=[true], groupBy=[sr_store_credit], select=[sr_store_credit, Final_MAX(max$0) AS EXPR$0])\
      +- [#2] Exchange(distribution=[hash[sr_store_credit]])\
])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[wr_returned_date_sk AS wr_returned_date_sk_node_10, wr_returned_time_sk AS wr_returned_time_sk_node_10, wr_item_sk AS wr_item_sk_node_10, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_10, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_10, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_10, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_10, wr_returning_customer_sk AS wr_returning_customer_sk_node_10, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_10, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_10, wr_returning_addr_sk AS wr_returning_addr_sk_node_10, wr_web_page_sk AS wr_web_page_sk_node_10, wr_reason_sk AS wr_reason_sk_node_10, wr_order_number AS wr_order_number_node_10, wr_return_quantity AS wr_return_quantity_node_10, wr_return_amt AS wr_return_amt_node_10, wr_return_tax AS wr_return_tax_node_10, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_10, wr_fee AS wr_fee_node_10, wr_return_ship_cost AS wr_return_ship_cost_node_10, wr_refunded_cash AS wr_refunded_cash_node_10, wr_reversed_charge AS wr_reversed_charge_node_10, wr_account_credit AS wr_account_credit_node_10, wr_net_loss AS wr_net_loss_node_10, 'hello' AS _c24])
         :     +- SortLimit(orderBy=[wr_returned_time_sk ASC], offset=[0], fetch=[45], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[wr_returned_time_sk ASC], offset=[0], fetch=[45], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
         +- Exchange(distribution=[hash[sr_store_credit]])
            +- LocalHashAggregate(groupBy=[sr_store_credit], select=[sr_store_credit, Partial_MAX(sr_reason_sk) AS max$0])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[(sr_return_time_sk = c_first_shipto_date_sk)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[right])
                  :- Sort(orderBy=[sr_return_amt_inc_tax ASC])
                  :  +- Exchange(distribution=[single])
                  :     +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                  +- Exchange(distribution=[broadcast])
                     +- Calc(select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], where=[(c_customer_sk <= -9)])
                        +- TableSourceScan(table=[[default_catalog, default_database, customer, filter=[<=(c_customer_sk, -9)]]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o158014323.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#319243530:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[13](input=RelSubset#319243528,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[13]), rel#319243527:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[13](input=RelSubset#319243526,groupBy=sr_return_time_sk, sr_store_credit,select=sr_return_time_sk, sr_store_credit, Partial_MAX(sr_reason_sk) AS max$0)]
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:250)
\tat org.apache.calcite.plan.volcano.IterativeRuleDriver.drive(IterativeRuleDriver.java:59)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.findBestExp(VolcanoPlanner.java:523)
\tat org.apache.calcite.tools.Programs$RuleSetProgram.run(Programs.java:317)
\tat org.apache.flink.table.planner.plan.optimize.program.FlinkVolcanoProgram.optimize(FlinkVolcanoProgram.scala:62)
\tat org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.$anonfun$optimize$1(FlinkChainedProgram.scala:59)
\tat scala.collection.TraversableOnce$folder$1.apply(TraversableOnce.scala:196)
\tat scala.collection.TraversableOnce$folder$1.apply(TraversableOnce.scala:194)
\tat scala.collection.Iterator.foreach(Iterator.scala:943)
\tat scala.collection.Iterator.foreach$(Iterator.scala:943)
\tat scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
\tat scala.collection.IterableLike.foreach(IterableLike.scala:74)
\tat scala.collection.IterableLike.foreach$(IterableLike.scala:73)
\tat scala.collection.AbstractIterable.foreach(Iterable.scala:56)
\tat scala.collection.TraversableOnce.foldLeft(TraversableOnce.scala:199)
\tat scala.collection.TraversableOnce.foldLeft$(TraversableOnce.scala:192)
\tat scala.collection.AbstractTraversable.foldLeft(Traversable.scala:108)
\tat org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.optimize(FlinkChainedProgram.scala:55)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.optimizeTree(BatchCommonSubGraphBasedOptimizer.scala:93)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.optimizeBlock(BatchCommonSubGraphBasedOptimizer.scala:58)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.$anonfun$doOptimize$1(BatchCommonSubGraphBasedOptimizer.scala:45)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.$anonfun$doOptimize$1$adapted(BatchCommonSubGraphBasedOptimizer.scala:45)
\tat scala.collection.immutable.List.foreach(List.scala:431)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.doOptimize(BatchCommonSubGraphBasedOptimizer.scala:45)
\tat org.apache.flink.table.planner.plan.optimize.CommonSubGraphBasedOptimizer.optimize(CommonSubGraphBasedOptimizer.scala:87)
\tat org.apache.flink.table.planner.delegation.PlannerBase.optimize(PlannerBase.scala:390)
\tat org.apache.flink.table.planner.delegation.PlannerBase.getExplainGraphs(PlannerBase.scala:625)
\tat org.apache.flink.table.planner.delegation.BatchPlanner.explain(BatchPlanner.scala:149)
\tat org.apache.flink.table.planner.delegation.BatchPlanner.explain(BatchPlanner.scala:49)
\tat org.apache.flink.table.api.internal.TableEnvironmentImpl.explainInternal(TableEnvironmentImpl.java:753)
\tat org.apache.flink.table.api.internal.TableImpl.explain(TableImpl.java:482)
\tat jdk.internal.reflect.GeneratedMethodAccessor32.invoke(Unknown Source)
\tat java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
\tat java.base/java.lang.reflect.Method.invoke(Method.java:566)
\tat org.apache.flink.api.python.shaded.py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
\tat org.apache.flink.api.python.shaded.py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
\tat org.apache.flink.api.python.shaded.py4j.Gateway.invoke(Gateway.java:282)
\tat org.apache.flink.api.python.shaded.py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
\tat org.apache.flink.api.python.shaded.py4j.commands.CallCommand.execute(CallCommand.java:79)
\tat org.apache.flink.api.python.shaded.py4j.GatewayConnection.run(GatewayConnection.java:238)
\tat java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.lang.RuntimeException: Error occurred while applying rule FlinkExpandConversionRule
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.transformTo(VolcanoRuleCall.java:157)
\tat org.apache.calcite.plan.RelOptRuleCall.transformTo(RelOptRuleCall.java:273)
\tat org.apache.calcite.plan.RelOptRuleCall.transformTo(RelOptRuleCall.java:288)
\tat org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule.satisfyTraitsBySelf(FlinkExpandConversionRule.scala:72)
\tat org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule.onMatch(FlinkExpandConversionRule.scala:52)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:223)
\t... 40 more
Caused by: java.lang.ArrayIndexOutOfBoundsException
",
      "stdout": "",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0