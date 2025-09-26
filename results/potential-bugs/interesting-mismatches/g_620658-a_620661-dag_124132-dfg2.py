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
    return values.skew()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_12 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_11 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_7 = autonode_10.order_by(col('ws_ext_discount_amt_node_10'))
autonode_9 = autonode_12.filter(col('wr_return_amt_node_12') > -23.598164319992065)
autonode_8 = autonode_11.add_columns(lit("hello"))
autonode_5 = autonode_7.filter(preloaded_udf_boolean(col('ws_bill_addr_sk_node_10')))
autonode_6 = autonode_8.join(autonode_9, col('wr_return_amt_node_12') == col('ws_ext_sales_price_node_11'))
autonode_4 = autonode_5.join(autonode_6, col('ws_sold_date_sk_node_11') == col('ws_quantity_node_10'))
autonode_3 = autonode_4.group_by(col('ws_sales_price_node_10')).select(col('ws_net_paid_inc_ship_tax_node_10').min.alias('ws_net_paid_inc_ship_tax_node_10'))
autonode_2 = autonode_3.filter(col('ws_net_paid_inc_ship_tax_node_10') < 32.31673836708069)
autonode_1 = autonode_2.filter(preloaded_udf_boolean(col('ws_net_paid_inc_ship_tax_node_10')))
sink = autonode_1.group_by(col('ws_net_paid_inc_ship_tax_node_10')).select(col('ws_net_paid_inc_ship_tax_node_10').avg.alias('ws_net_paid_inc_ship_tax_node_10'))
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
LogicalProject(ws_net_paid_inc_ship_tax_node_10=[$1])
+- LogicalAggregate(group=[{0}], EXPR$0=[AVG($0)])
   +- LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($0)])
      +- LogicalFilter(condition=[<($0, 3.231673836708069E1:DOUBLE)])
         +- LogicalProject(ws_net_paid_inc_ship_tax_node_10=[$1])
            +- LogicalAggregate(group=[{21}], EXPR$0=[MIN($32)])
               +- LogicalJoin(condition=[=($34, $18)], joinType=[inner])
                  :- LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($7)])
                  :  +- LogicalSort(sort0=[$22], dir0=[ASC])
                  :     +- LogicalProject(ws_sold_date_sk_node_10=[$0], ws_sold_time_sk_node_10=[$1], ws_ship_date_sk_node_10=[$2], ws_item_sk_node_10=[$3], ws_bill_customer_sk_node_10=[$4], ws_bill_cdemo_sk_node_10=[$5], ws_bill_hdemo_sk_node_10=[$6], ws_bill_addr_sk_node_10=[$7], ws_ship_customer_sk_node_10=[$8], ws_ship_cdemo_sk_node_10=[$9], ws_ship_hdemo_sk_node_10=[$10], ws_ship_addr_sk_node_10=[$11], ws_web_page_sk_node_10=[$12], ws_web_site_sk_node_10=[$13], ws_ship_mode_sk_node_10=[$14], ws_warehouse_sk_node_10=[$15], ws_promo_sk_node_10=[$16], ws_order_number_node_10=[$17], ws_quantity_node_10=[$18], ws_wholesale_cost_node_10=[$19], ws_list_price_node_10=[$20], ws_sales_price_node_10=[$21], ws_ext_discount_amt_node_10=[$22], ws_ext_sales_price_node_10=[$23], ws_ext_wholesale_cost_node_10=[$24], ws_ext_list_price_node_10=[$25], ws_ext_tax_node_10=[$26], ws_coupon_amt_node_10=[$27], ws_ext_ship_cost_node_10=[$28], ws_net_paid_node_10=[$29], ws_net_paid_inc_tax_node_10=[$30], ws_net_paid_inc_ship_node_10=[$31], ws_net_paid_inc_ship_tax_node_10=[$32], ws_net_profit_node_10=[$33])
                  :        +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
                  +- LogicalJoin(condition=[=($50, $23)], joinType=[inner])
                     :- LogicalProject(ws_sold_date_sk_node_11=[$0], ws_sold_time_sk_node_11=[$1], ws_ship_date_sk_node_11=[$2], ws_item_sk_node_11=[$3], ws_bill_customer_sk_node_11=[$4], ws_bill_cdemo_sk_node_11=[$5], ws_bill_hdemo_sk_node_11=[$6], ws_bill_addr_sk_node_11=[$7], ws_ship_customer_sk_node_11=[$8], ws_ship_cdemo_sk_node_11=[$9], ws_ship_hdemo_sk_node_11=[$10], ws_ship_addr_sk_node_11=[$11], ws_web_page_sk_node_11=[$12], ws_web_site_sk_node_11=[$13], ws_ship_mode_sk_node_11=[$14], ws_warehouse_sk_node_11=[$15], ws_promo_sk_node_11=[$16], ws_order_number_node_11=[$17], ws_quantity_node_11=[$18], ws_wholesale_cost_node_11=[$19], ws_list_price_node_11=[$20], ws_sales_price_node_11=[$21], ws_ext_discount_amt_node_11=[$22], ws_ext_sales_price_node_11=[$23], ws_ext_wholesale_cost_node_11=[$24], ws_ext_list_price_node_11=[$25], ws_ext_tax_node_11=[$26], ws_coupon_amt_node_11=[$27], ws_ext_ship_cost_node_11=[$28], ws_net_paid_node_11=[$29], ws_net_paid_inc_tax_node_11=[$30], ws_net_paid_inc_ship_node_11=[$31], ws_net_paid_inc_ship_tax_node_11=[$32], ws_net_profit_node_11=[$33], _c34=[_UTF-16LE'hello'])
                     :  +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
                     +- LogicalFilter(condition=[>($15, -2.3598164319992065E1:DOUBLE)])
                        +- LogicalProject(wr_returned_date_sk_node_12=[$0], wr_returned_time_sk_node_12=[$1], wr_item_sk_node_12=[$2], wr_refunded_customer_sk_node_12=[$3], wr_refunded_cdemo_sk_node_12=[$4], wr_refunded_hdemo_sk_node_12=[$5], wr_refunded_addr_sk_node_12=[$6], wr_returning_customer_sk_node_12=[$7], wr_returning_cdemo_sk_node_12=[$8], wr_returning_hdemo_sk_node_12=[$9], wr_returning_addr_sk_node_12=[$10], wr_web_page_sk_node_12=[$11], wr_reason_sk_node_12=[$12], wr_order_number_node_12=[$13], wr_return_quantity_node_12=[$14], wr_return_amt_node_12=[$15], wr_return_tax_node_12=[$16], wr_return_amt_inc_tax_node_12=[$17], wr_fee_node_12=[$18], wr_return_ship_cost_node_12=[$19], wr_refunded_cash_node_12=[$20], wr_reversed_charge_node_12=[$21], wr_account_credit_node_12=[$22], wr_net_loss_node_12=[$23])
                           +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])

== Optimized Physical Plan ==
Calc(select=[CAST(EXPR$0 AS DECIMAL(38, 6)) AS ws_net_paid_inc_ship_tax_node_10])
+- HashAggregate(isMerge=[true], groupBy=[EXPR$0], select=[EXPR$0])
   +- Exchange(distribution=[hash[EXPR$0]])
      +- LocalHashAggregate(groupBy=[EXPR$0], select=[EXPR$0])
         +- Calc(select=[EXPR$0], where=[AND(<(EXPR$0, 3.231673836708069E1), f0)])
            +- PythonCalc(select=[EXPR$0, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(EXPR$0) AS f0])
               +- HashAggregate(isMerge=[true], groupBy=[ws_sales_price], select=[ws_sales_price, Final_MIN(min$0) AS EXPR$0])
                  +- Exchange(distribution=[hash[ws_sales_price]])
                     +- LocalHashAggregate(groupBy=[ws_sales_price], select=[ws_sales_price, Partial_MIN(ws_net_paid_inc_ship_tax) AS min$0])
                        +- HashJoin(joinType=[InnerJoin], where=[=(ws_sold_date_sk_node_11, ws_quantity)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, ws_sold_date_sk_node_11, ws_sold_time_sk_node_11, ws_ship_date_sk_node_11, ws_item_sk_node_11, ws_bill_customer_sk_node_11, ws_bill_cdemo_sk_node_11, ws_bill_hdemo_sk_node_11, ws_bill_addr_sk_node_11, ws_ship_customer_sk_node_11, ws_ship_cdemo_sk_node_11, ws_ship_hdemo_sk_node_11, ws_ship_addr_sk_node_11, ws_web_page_sk_node_11, ws_web_site_sk_node_11, ws_ship_mode_sk_node_11, ws_warehouse_sk_node_11, ws_promo_sk_node_11, ws_order_number_node_11, ws_quantity_node_11, ws_wholesale_cost_node_11, ws_list_price_node_11, ws_sales_price_node_11, ws_ext_discount_amt_node_11, ws_ext_sales_price_node_11, ws_ext_wholesale_cost_node_11, ws_ext_list_price_node_11, ws_ext_tax_node_11, ws_coupon_amt_node_11, ws_ext_ship_cost_node_11, ws_net_paid_node_11, ws_net_paid_inc_tax_node_11, ws_net_paid_inc_ship_node_11, ws_net_paid_inc_ship_tax_node_11, ws_net_profit_node_11, _c34, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], build=[left])
                           :- Exchange(distribution=[hash[ws_quantity]])
                           :  +- Calc(select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], where=[f0])
                           :     +- PythonCalc(select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(ws_bill_addr_sk) AS f0])
                           :        +- Sort(orderBy=[ws_ext_discount_amt ASC])
                           :           +- Exchange(distribution=[single])
                           :              +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                           +- Exchange(distribution=[hash[ws_sold_date_sk_node_11]])
                              +- HashJoin(joinType=[InnerJoin], where=[=(wr_return_amt, ws_ext_sales_price_node_11)], select=[ws_sold_date_sk_node_11, ws_sold_time_sk_node_11, ws_ship_date_sk_node_11, ws_item_sk_node_11, ws_bill_customer_sk_node_11, ws_bill_cdemo_sk_node_11, ws_bill_hdemo_sk_node_11, ws_bill_addr_sk_node_11, ws_ship_customer_sk_node_11, ws_ship_cdemo_sk_node_11, ws_ship_hdemo_sk_node_11, ws_ship_addr_sk_node_11, ws_web_page_sk_node_11, ws_web_site_sk_node_11, ws_ship_mode_sk_node_11, ws_warehouse_sk_node_11, ws_promo_sk_node_11, ws_order_number_node_11, ws_quantity_node_11, ws_wholesale_cost_node_11, ws_list_price_node_11, ws_sales_price_node_11, ws_ext_discount_amt_node_11, ws_ext_sales_price_node_11, ws_ext_wholesale_cost_node_11, ws_ext_list_price_node_11, ws_ext_tax_node_11, ws_coupon_amt_node_11, ws_ext_ship_cost_node_11, ws_net_paid_node_11, ws_net_paid_inc_tax_node_11, ws_net_paid_inc_ship_node_11, ws_net_paid_inc_ship_tax_node_11, ws_net_profit_node_11, _c34, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], build=[right])
                                 :- Calc(select=[ws_sold_date_sk AS ws_sold_date_sk_node_11, ws_sold_time_sk AS ws_sold_time_sk_node_11, ws_ship_date_sk AS ws_ship_date_sk_node_11, ws_item_sk AS ws_item_sk_node_11, ws_bill_customer_sk AS ws_bill_customer_sk_node_11, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_11, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_11, ws_bill_addr_sk AS ws_bill_addr_sk_node_11, ws_ship_customer_sk AS ws_ship_customer_sk_node_11, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_11, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_11, ws_ship_addr_sk AS ws_ship_addr_sk_node_11, ws_web_page_sk AS ws_web_page_sk_node_11, ws_web_site_sk AS ws_web_site_sk_node_11, ws_ship_mode_sk AS ws_ship_mode_sk_node_11, ws_warehouse_sk AS ws_warehouse_sk_node_11, ws_promo_sk AS ws_promo_sk_node_11, ws_order_number AS ws_order_number_node_11, ws_quantity AS ws_quantity_node_11, ws_wholesale_cost AS ws_wholesale_cost_node_11, ws_list_price AS ws_list_price_node_11, ws_sales_price AS ws_sales_price_node_11, ws_ext_discount_amt AS ws_ext_discount_amt_node_11, ws_ext_sales_price AS ws_ext_sales_price_node_11, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_11, ws_ext_list_price AS ws_ext_list_price_node_11, ws_ext_tax AS ws_ext_tax_node_11, ws_coupon_amt AS ws_coupon_amt_node_11, ws_ext_ship_cost AS ws_ext_ship_cost_node_11, ws_net_paid AS ws_net_paid_node_11, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_11, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_11, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_11, ws_net_profit AS ws_net_profit_node_11, 'hello' AS _c34])
                                 :  +- Exchange(distribution=[hash[ws_ext_sales_price]])
                                 :     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                                 +- Exchange(distribution=[hash[wr_return_amt]])
                                    +- Calc(select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], where=[>(wr_return_amt, -2.3598164319992065E1)])
                                       +- TableSourceScan(table=[[default_catalog, default_database, web_returns, filter=[>(wr_return_amt, -2.3598164319992065E1:DOUBLE)]]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])

== Optimized Execution Plan ==
Calc(select=[CAST(EXPR$0 AS DECIMAL(38, 6)) AS ws_net_paid_inc_ship_tax_node_10])
+- HashAggregate(isMerge=[true], groupBy=[EXPR$0], select=[EXPR$0])
   +- Exchange(distribution=[hash[EXPR$0]])
      +- LocalHashAggregate(groupBy=[EXPR$0], select=[EXPR$0])
         +- Calc(select=[EXPR$0], where=[((EXPR$0 < 3.231673836708069E1) AND f0)])
            +- PythonCalc(select=[EXPR$0, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(EXPR$0) AS f0])
               +- HashAggregate(isMerge=[true], groupBy=[ws_sales_price], select=[ws_sales_price, Final_MIN(min$0) AS EXPR$0])
                  +- Exchange(distribution=[hash[ws_sales_price]])
                     +- LocalHashAggregate(groupBy=[ws_sales_price], select=[ws_sales_price, Partial_MIN(ws_net_paid_inc_ship_tax) AS min$0])
                        +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ws_sold_date_sk_node_11 = ws_quantity)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, ws_sold_date_sk_node_11, ws_sold_time_sk_node_11, ws_ship_date_sk_node_11, ws_item_sk_node_11, ws_bill_customer_sk_node_11, ws_bill_cdemo_sk_node_11, ws_bill_hdemo_sk_node_11, ws_bill_addr_sk_node_11, ws_ship_customer_sk_node_11, ws_ship_cdemo_sk_node_11, ws_ship_hdemo_sk_node_11, ws_ship_addr_sk_node_11, ws_web_page_sk_node_11, ws_web_site_sk_node_11, ws_ship_mode_sk_node_11, ws_warehouse_sk_node_11, ws_promo_sk_node_11, ws_order_number_node_11, ws_quantity_node_11, ws_wholesale_cost_node_11, ws_list_price_node_11, ws_sales_price_node_11, ws_ext_discount_amt_node_11, ws_ext_sales_price_node_11, ws_ext_wholesale_cost_node_11, ws_ext_list_price_node_11, ws_ext_tax_node_11, ws_coupon_amt_node_11, ws_ext_ship_cost_node_11, ws_net_paid_node_11, ws_net_paid_inc_tax_node_11, ws_net_paid_inc_ship_node_11, ws_net_paid_inc_ship_tax_node_11, ws_net_profit_node_11, _c34, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], build=[left])
                           :- Exchange(distribution=[hash[ws_quantity]])
                           :  +- Calc(select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], where=[f0])
                           :     +- PythonCalc(select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(ws_bill_addr_sk) AS f0])
                           :        +- Sort(orderBy=[ws_ext_discount_amt ASC])
                           :           +- Exchange(distribution=[single])
                           :              +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])(reuse_id=[1])
                           +- Exchange(distribution=[hash[ws_sold_date_sk_node_11]])
                              +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(wr_return_amt = ws_ext_sales_price_node_11)], select=[ws_sold_date_sk_node_11, ws_sold_time_sk_node_11, ws_ship_date_sk_node_11, ws_item_sk_node_11, ws_bill_customer_sk_node_11, ws_bill_cdemo_sk_node_11, ws_bill_hdemo_sk_node_11, ws_bill_addr_sk_node_11, ws_ship_customer_sk_node_11, ws_ship_cdemo_sk_node_11, ws_ship_hdemo_sk_node_11, ws_ship_addr_sk_node_11, ws_web_page_sk_node_11, ws_web_site_sk_node_11, ws_ship_mode_sk_node_11, ws_warehouse_sk_node_11, ws_promo_sk_node_11, ws_order_number_node_11, ws_quantity_node_11, ws_wholesale_cost_node_11, ws_list_price_node_11, ws_sales_price_node_11, ws_ext_discount_amt_node_11, ws_ext_sales_price_node_11, ws_ext_wholesale_cost_node_11, ws_ext_list_price_node_11, ws_ext_tax_node_11, ws_coupon_amt_node_11, ws_ext_ship_cost_node_11, ws_net_paid_node_11, ws_net_paid_inc_tax_node_11, ws_net_paid_inc_ship_node_11, ws_net_paid_inc_ship_tax_node_11, ws_net_profit_node_11, _c34, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], build=[right])\
:- Calc(select=[ws_sold_date_sk AS ws_sold_date_sk_node_11, ws_sold_time_sk AS ws_sold_time_sk_node_11, ws_ship_date_sk AS ws_ship_date_sk_node_11, ws_item_sk AS ws_item_sk_node_11, ws_bill_customer_sk AS ws_bill_customer_sk_node_11, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_11, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_11, ws_bill_addr_sk AS ws_bill_addr_sk_node_11, ws_ship_customer_sk AS ws_ship_customer_sk_node_11, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_11, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_11, ws_ship_addr_sk AS ws_ship_addr_sk_node_11, ws_web_page_sk AS ws_web_page_sk_node_11, ws_web_site_sk AS ws_web_site_sk_node_11, ws_ship_mode_sk AS ws_ship_mode_sk_node_11, ws_warehouse_sk AS ws_warehouse_sk_node_11, ws_promo_sk AS ws_promo_sk_node_11, ws_order_number AS ws_order_number_node_11, ws_quantity AS ws_quantity_node_11, ws_wholesale_cost AS ws_wholesale_cost_node_11, ws_list_price AS ws_list_price_node_11, ws_sales_price AS ws_sales_price_node_11, ws_ext_discount_amt AS ws_ext_discount_amt_node_11, ws_ext_sales_price AS ws_ext_sales_price_node_11, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_11, ws_ext_list_price AS ws_ext_list_price_node_11, ws_ext_tax AS ws_ext_tax_node_11, ws_coupon_amt AS ws_coupon_amt_node_11, ws_ext_ship_cost AS ws_ext_ship_cost_node_11, ws_net_paid AS ws_net_paid_node_11, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_11, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_11, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_11, ws_net_profit AS ws_net_profit_node_11, 'hello' AS _c34])\
:  +- [#2] Exchange(distribution=[hash[ws_ext_sales_price]])\
+- [#1] Exchange(distribution=[hash[wr_return_amt]])\
])
                                 :- Exchange(distribution=[hash[wr_return_amt]])
                                 :  +- Calc(select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], where=[(wr_return_amt > -2.3598164319992065E1)])
                                 :     +- TableSourceScan(table=[[default_catalog, default_database, web_returns, filter=[>(wr_return_amt, -2.3598164319992065E1:DOUBLE)]]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
                                 +- Exchange(distribution=[hash[ws_ext_sales_price]])
                                    +- Reused(reference_id=[1])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o338529331.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#681951866:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[22](input=RelSubset#681951864,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[22]), rel#681951863:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[22](input=RelSubset#681951862,groupBy=ws_quantity, ws_sales_price,select=ws_quantity, ws_sales_price, Partial_MIN(ws_net_paid_inc_ship_tax) AS min$0)]
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