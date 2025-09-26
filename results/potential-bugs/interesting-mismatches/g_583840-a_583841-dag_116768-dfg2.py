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

autonode_13 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_12 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_11 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_10 = autonode_13.add_columns(lit("hello"))
autonode_9 = autonode_12.order_by(col('ws_list_price_node_12'))
autonode_8 = autonode_11.group_by(col('i_class_id_node_11')).select(col('i_wholesale_cost_node_11').count.alias('i_wholesale_cost_node_11'))
autonode_7 = autonode_10.alias('oqYaC')
autonode_6 = autonode_8.join(autonode_9, col('i_wholesale_cost_node_11') == col('ws_ext_list_price_node_12'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_4 = autonode_6.group_by(col('ws_bill_cdemo_sk_node_12')).select(col('ws_order_number_node_12').min.alias('ws_order_number_node_12'))
autonode_3 = autonode_4.join(autonode_5, col('sr_return_quantity_node_13') == col('ws_order_number_node_12'))
autonode_2 = autonode_3.group_by(col('sr_return_tax_node_13')).select(col('sr_customer_sk_node_13').min.alias('sr_customer_sk_node_13'))
autonode_1 = autonode_2.limit(88)
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
LogicalProject(sr_customer_sk_node_13=[$0], _c1=[_UTF-16LE'hello'])
+- LogicalSort(fetch=[88])
   +- LogicalProject(sr_customer_sk_node_13=[$1])
      +- LogicalAggregate(group=[{13}], EXPR$0=[MIN($4)])
         +- LogicalJoin(condition=[=($11, $0)], joinType=[inner])
            :- LogicalProject(ws_order_number_node_12=[$1])
            :  +- LogicalAggregate(group=[{6}], EXPR$0=[MIN($18)])
            :     +- LogicalJoin(condition=[=($0, $26)], joinType=[inner])
            :        :- LogicalProject(i_wholesale_cost_node_11=[$1])
            :        :  +- LogicalAggregate(group=[{1}], EXPR$0=[COUNT($0)])
            :        :     +- LogicalProject(i_wholesale_cost_node_11=[$6], i_class_id_node_11=[$9])
            :        :        +- LogicalTableScan(table=[[default_catalog, default_database, item]])
            :        +- LogicalSort(sort0=[$20], dir0=[ASC])
            :           +- LogicalProject(ws_sold_date_sk_node_12=[$0], ws_sold_time_sk_node_12=[$1], ws_ship_date_sk_node_12=[$2], ws_item_sk_node_12=[$3], ws_bill_customer_sk_node_12=[$4], ws_bill_cdemo_sk_node_12=[$5], ws_bill_hdemo_sk_node_12=[$6], ws_bill_addr_sk_node_12=[$7], ws_ship_customer_sk_node_12=[$8], ws_ship_cdemo_sk_node_12=[$9], ws_ship_hdemo_sk_node_12=[$10], ws_ship_addr_sk_node_12=[$11], ws_web_page_sk_node_12=[$12], ws_web_site_sk_node_12=[$13], ws_ship_mode_sk_node_12=[$14], ws_warehouse_sk_node_12=[$15], ws_promo_sk_node_12=[$16], ws_order_number_node_12=[$17], ws_quantity_node_12=[$18], ws_wholesale_cost_node_12=[$19], ws_list_price_node_12=[$20], ws_sales_price_node_12=[$21], ws_ext_discount_amt_node_12=[$22], ws_ext_sales_price_node_12=[$23], ws_ext_wholesale_cost_node_12=[$24], ws_ext_list_price_node_12=[$25], ws_ext_tax_node_12=[$26], ws_coupon_amt_node_12=[$27], ws_ext_ship_cost_node_12=[$28], ws_net_paid_node_12=[$29], ws_net_paid_inc_tax_node_12=[$30], ws_net_paid_inc_ship_node_12=[$31], ws_net_paid_inc_ship_tax_node_12=[$32], ws_net_profit_node_12=[$33])
            :              +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
            +- LogicalProject(oqYaC=[AS($0, _UTF-16LE'oqYaC')], sr_return_time_sk_node_13=[$1], sr_item_sk_node_13=[$2], sr_customer_sk_node_13=[$3], sr_cdemo_sk_node_13=[$4], sr_hdemo_sk_node_13=[$5], sr_addr_sk_node_13=[$6], sr_store_sk_node_13=[$7], sr_reason_sk_node_13=[$8], sr_ticket_number_node_13=[$9], sr_return_quantity_node_13=[$10], sr_return_amt_node_13=[$11], sr_return_tax_node_13=[$12], sr_return_amt_inc_tax_node_13=[$13], sr_fee_node_13=[$14], sr_return_ship_cost_node_13=[$15], sr_refunded_cash_node_13=[$16], sr_reversed_charge_node_13=[$17], sr_store_credit_node_13=[$18], sr_net_loss_node_13=[$19], _c20=[_UTF-16LE'hello'], _c21=[_UTF-16LE'hello'])
               +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS sr_customer_sk_node_13, 'hello' AS _c1])
+- Limit(offset=[0], fetch=[88], global=[true])
   +- Exchange(distribution=[single])
      +- Limit(offset=[0], fetch=[88], global=[false])
         +- HashAggregate(isMerge=[true], groupBy=[sr_return_tax_node_13], select=[sr_return_tax_node_13, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[hash[sr_return_tax_node_13]])
               +- LocalHashAggregate(groupBy=[sr_return_tax_node_13], select=[sr_return_tax_node_13, Partial_MIN(sr_customer_sk_node_13) AS min$0])
                  +- HashJoin(joinType=[InnerJoin], where=[=(sr_return_quantity_node_13, ws_order_number_node_12)], select=[ws_order_number_node_12, oqYaC, sr_return_time_sk_node_13, sr_item_sk_node_13, sr_customer_sk_node_13, sr_cdemo_sk_node_13, sr_hdemo_sk_node_13, sr_addr_sk_node_13, sr_store_sk_node_13, sr_reason_sk_node_13, sr_ticket_number_node_13, sr_return_quantity_node_13, sr_return_amt_node_13, sr_return_tax_node_13, sr_return_amt_inc_tax_node_13, sr_fee_node_13, sr_return_ship_cost_node_13, sr_refunded_cash_node_13, sr_reversed_charge_node_13, sr_store_credit_node_13, sr_net_loss_node_13, _c20, _c21], isBroadcast=[true], build=[left])
                     :- Exchange(distribution=[broadcast])
                     :  +- Calc(select=[EXPR$0 AS ws_order_number_node_12])
                     :     +- HashAggregate(isMerge=[true], groupBy=[ws_bill_cdemo_sk_node_12], select=[ws_bill_cdemo_sk_node_12, Final_MIN(min$0) AS EXPR$0])
                     :        +- Exchange(distribution=[hash[ws_bill_cdemo_sk_node_12]])
                     :           +- LocalHashAggregate(groupBy=[ws_bill_cdemo_sk_node_12], select=[ws_bill_cdemo_sk_node_12, Partial_MIN(ws_order_number_node_12) AS min$0])
                     :              +- Calc(select=[i_wholesale_cost_node_11, ws_sold_date_sk_node_12, ws_sold_time_sk_node_12, ws_ship_date_sk_node_12, ws_item_sk_node_12, ws_bill_customer_sk_node_12, ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk_node_12, ws_bill_addr_sk_node_12, ws_ship_customer_sk_node_12, ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk_node_12, ws_ship_addr_sk_node_12, ws_web_page_sk_node_12, ws_web_site_sk_node_12, ws_ship_mode_sk_node_12, ws_warehouse_sk_node_12, ws_promo_sk_node_12, ws_order_number_node_12, ws_quantity_node_12, ws_wholesale_cost_node_12, ws_list_price_node_12, ws_sales_price_node_12, ws_ext_discount_amt_node_12, ws_ext_sales_price_node_12, ws_ext_wholesale_cost_node_12, ws_ext_list_price_node_12, ws_ext_tax_node_12, ws_coupon_amt_node_12, ws_ext_ship_cost_node_12, ws_net_paid_node_12, ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax_node_12, ws_net_profit_node_12])
                     :                 +- HashJoin(joinType=[InnerJoin], where=[=(i_wholesale_cost_node_110, ws_ext_list_price_node_120)], select=[i_wholesale_cost_node_11, i_wholesale_cost_node_110, ws_sold_date_sk_node_12, ws_sold_time_sk_node_12, ws_ship_date_sk_node_12, ws_item_sk_node_12, ws_bill_customer_sk_node_12, ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk_node_12, ws_bill_addr_sk_node_12, ws_ship_customer_sk_node_12, ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk_node_12, ws_ship_addr_sk_node_12, ws_web_page_sk_node_12, ws_web_site_sk_node_12, ws_ship_mode_sk_node_12, ws_warehouse_sk_node_12, ws_promo_sk_node_12, ws_order_number_node_12, ws_quantity_node_12, ws_wholesale_cost_node_12, ws_list_price_node_12, ws_sales_price_node_12, ws_ext_discount_amt_node_12, ws_ext_sales_price_node_12, ws_ext_wholesale_cost_node_12, ws_ext_list_price_node_12, ws_ext_tax_node_12, ws_coupon_amt_node_12, ws_ext_ship_cost_node_12, ws_net_paid_node_12, ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax_node_12, ws_net_profit_node_12, ws_ext_list_price_node_120], isBroadcast=[true], build=[left])
                     :                    :- Exchange(distribution=[broadcast])
                     :                    :  +- Calc(select=[EXPR$0 AS i_wholesale_cost_node_11, CAST(EXPR$0 AS DECIMAL(21, 2)) AS i_wholesale_cost_node_110])
                     :                    :     +- HashAggregate(isMerge=[true], groupBy=[i_class_id], select=[i_class_id, Final_COUNT(count$0) AS EXPR$0])
                     :                    :        +- Exchange(distribution=[hash[i_class_id]])
                     :                    :           +- LocalHashAggregate(groupBy=[i_class_id], select=[i_class_id, Partial_COUNT(i_wholesale_cost) AS count$0])
                     :                    :              +- TableSourceScan(table=[[default_catalog, default_database, item, project=[i_wholesale_cost, i_class_id], metadata=[]]], fields=[i_wholesale_cost, i_class_id])
                     :                    +- Calc(select=[ws_sold_date_sk AS ws_sold_date_sk_node_12, ws_sold_time_sk AS ws_sold_time_sk_node_12, ws_ship_date_sk AS ws_ship_date_sk_node_12, ws_item_sk AS ws_item_sk_node_12, ws_bill_customer_sk AS ws_bill_customer_sk_node_12, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_12, ws_bill_addr_sk AS ws_bill_addr_sk_node_12, ws_ship_customer_sk AS ws_ship_customer_sk_node_12, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_12, ws_ship_addr_sk AS ws_ship_addr_sk_node_12, ws_web_page_sk AS ws_web_page_sk_node_12, ws_web_site_sk AS ws_web_site_sk_node_12, ws_ship_mode_sk AS ws_ship_mode_sk_node_12, ws_warehouse_sk AS ws_warehouse_sk_node_12, ws_promo_sk AS ws_promo_sk_node_12, ws_order_number AS ws_order_number_node_12, ws_quantity AS ws_quantity_node_12, ws_wholesale_cost AS ws_wholesale_cost_node_12, ws_list_price AS ws_list_price_node_12, ws_sales_price AS ws_sales_price_node_12, ws_ext_discount_amt AS ws_ext_discount_amt_node_12, ws_ext_sales_price AS ws_ext_sales_price_node_12, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_12, ws_ext_list_price AS ws_ext_list_price_node_12, ws_ext_tax AS ws_ext_tax_node_12, ws_coupon_amt AS ws_coupon_amt_node_12, ws_ext_ship_cost AS ws_ext_ship_cost_node_12, ws_net_paid AS ws_net_paid_node_12, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_12, ws_net_profit AS ws_net_profit_node_12, CAST(ws_ext_list_price AS DECIMAL(21, 2)) AS ws_ext_list_price_node_120])
                     :                       +- Sort(orderBy=[ws_list_price ASC])
                     :                          +- Exchange(distribution=[single])
                     :                             +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                     +- Calc(select=[sr_returned_date_sk AS oqYaC, sr_return_time_sk AS sr_return_time_sk_node_13, sr_item_sk AS sr_item_sk_node_13, sr_customer_sk AS sr_customer_sk_node_13, sr_cdemo_sk AS sr_cdemo_sk_node_13, sr_hdemo_sk AS sr_hdemo_sk_node_13, sr_addr_sk AS sr_addr_sk_node_13, sr_store_sk AS sr_store_sk_node_13, sr_reason_sk AS sr_reason_sk_node_13, sr_ticket_number AS sr_ticket_number_node_13, sr_return_quantity AS sr_return_quantity_node_13, sr_return_amt AS sr_return_amt_node_13, sr_return_tax AS sr_return_tax_node_13, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_13, sr_fee AS sr_fee_node_13, sr_return_ship_cost AS sr_return_ship_cost_node_13, sr_refunded_cash AS sr_refunded_cash_node_13, sr_reversed_charge AS sr_reversed_charge_node_13, sr_store_credit AS sr_store_credit_node_13, sr_net_loss AS sr_net_loss_node_13, 'hello' AS _c20, 'hello' AS _c21])
                        +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS sr_customer_sk_node_13, 'hello' AS _c1])
+- Limit(offset=[0], fetch=[88], global=[true])
   +- Exchange(distribution=[single])
      +- Limit(offset=[0], fetch=[88], global=[false])
         +- HashAggregate(isMerge=[true], groupBy=[sr_return_tax_node_13], select=[sr_return_tax_node_13, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[hash[sr_return_tax_node_13]])
               +- LocalHashAggregate(groupBy=[sr_return_tax_node_13], select=[sr_return_tax_node_13, Partial_MIN(sr_customer_sk_node_13) AS min$0])
                  +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(sr_return_quantity_node_13 = ws_order_number_node_12)], select=[ws_order_number_node_12, oqYaC, sr_return_time_sk_node_13, sr_item_sk_node_13, sr_customer_sk_node_13, sr_cdemo_sk_node_13, sr_hdemo_sk_node_13, sr_addr_sk_node_13, sr_store_sk_node_13, sr_reason_sk_node_13, sr_ticket_number_node_13, sr_return_quantity_node_13, sr_return_amt_node_13, sr_return_tax_node_13, sr_return_amt_inc_tax_node_13, sr_fee_node_13, sr_return_ship_cost_node_13, sr_refunded_cash_node_13, sr_reversed_charge_node_13, sr_store_credit_node_13, sr_net_loss_node_13, _c20, _c21], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- Calc(select=[sr_returned_date_sk AS oqYaC, sr_return_time_sk AS sr_return_time_sk_node_13, sr_item_sk AS sr_item_sk_node_13, sr_customer_sk AS sr_customer_sk_node_13, sr_cdemo_sk AS sr_cdemo_sk_node_13, sr_hdemo_sk AS sr_hdemo_sk_node_13, sr_addr_sk AS sr_addr_sk_node_13, sr_store_sk AS sr_store_sk_node_13, sr_reason_sk AS sr_reason_sk_node_13, sr_ticket_number AS sr_ticket_number_node_13, sr_return_quantity AS sr_return_quantity_node_13, sr_return_amt AS sr_return_amt_node_13, sr_return_tax AS sr_return_tax_node_13, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_13, sr_fee AS sr_fee_node_13, sr_return_ship_cost AS sr_return_ship_cost_node_13, sr_refunded_cash AS sr_refunded_cash_node_13, sr_reversed_charge AS sr_reversed_charge_node_13, sr_store_credit AS sr_store_credit_node_13, sr_net_loss AS sr_net_loss_node_13, 'hello' AS _c20, 'hello' AS _c21])\
   +- [#2] TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])\
])
                     :- Exchange(distribution=[broadcast])
                     :  +- Calc(select=[EXPR$0 AS ws_order_number_node_12])
                     :     +- HashAggregate(isMerge=[true], groupBy=[ws_bill_cdemo_sk_node_12], select=[ws_bill_cdemo_sk_node_12, Final_MIN(min$0) AS EXPR$0])
                     :        +- Exchange(distribution=[hash[ws_bill_cdemo_sk_node_12]])
                     :           +- LocalHashAggregate(groupBy=[ws_bill_cdemo_sk_node_12], select=[ws_bill_cdemo_sk_node_12, Partial_MIN(ws_order_number_node_12) AS min$0])
                     :              +- Calc(select=[i_wholesale_cost_node_11, ws_sold_date_sk_node_12, ws_sold_time_sk_node_12, ws_ship_date_sk_node_12, ws_item_sk_node_12, ws_bill_customer_sk_node_12, ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk_node_12, ws_bill_addr_sk_node_12, ws_ship_customer_sk_node_12, ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk_node_12, ws_ship_addr_sk_node_12, ws_web_page_sk_node_12, ws_web_site_sk_node_12, ws_ship_mode_sk_node_12, ws_warehouse_sk_node_12, ws_promo_sk_node_12, ws_order_number_node_12, ws_quantity_node_12, ws_wholesale_cost_node_12, ws_list_price_node_12, ws_sales_price_node_12, ws_ext_discount_amt_node_12, ws_ext_sales_price_node_12, ws_ext_wholesale_cost_node_12, ws_ext_list_price_node_12, ws_ext_tax_node_12, ws_coupon_amt_node_12, ws_ext_ship_cost_node_12, ws_net_paid_node_12, ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax_node_12, ws_net_profit_node_12])
                     :                 +- HashJoin(joinType=[InnerJoin], where=[(i_wholesale_cost_node_110 = ws_ext_list_price_node_120)], select=[i_wholesale_cost_node_11, i_wholesale_cost_node_110, ws_sold_date_sk_node_12, ws_sold_time_sk_node_12, ws_ship_date_sk_node_12, ws_item_sk_node_12, ws_bill_customer_sk_node_12, ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk_node_12, ws_bill_addr_sk_node_12, ws_ship_customer_sk_node_12, ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk_node_12, ws_ship_addr_sk_node_12, ws_web_page_sk_node_12, ws_web_site_sk_node_12, ws_ship_mode_sk_node_12, ws_warehouse_sk_node_12, ws_promo_sk_node_12, ws_order_number_node_12, ws_quantity_node_12, ws_wholesale_cost_node_12, ws_list_price_node_12, ws_sales_price_node_12, ws_ext_discount_amt_node_12, ws_ext_sales_price_node_12, ws_ext_wholesale_cost_node_12, ws_ext_list_price_node_12, ws_ext_tax_node_12, ws_coupon_amt_node_12, ws_ext_ship_cost_node_12, ws_net_paid_node_12, ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax_node_12, ws_net_profit_node_12, ws_ext_list_price_node_120], isBroadcast=[true], build=[left])
                     :                    :- Exchange(distribution=[broadcast])
                     :                    :  +- Calc(select=[EXPR$0 AS i_wholesale_cost_node_11, CAST(EXPR$0 AS DECIMAL(21, 2)) AS i_wholesale_cost_node_110])
                     :                    :     +- HashAggregate(isMerge=[true], groupBy=[i_class_id], select=[i_class_id, Final_COUNT(count$0) AS EXPR$0])
                     :                    :        +- Exchange(distribution=[hash[i_class_id]])
                     :                    :           +- LocalHashAggregate(groupBy=[i_class_id], select=[i_class_id, Partial_COUNT(i_wholesale_cost) AS count$0])
                     :                    :              +- TableSourceScan(table=[[default_catalog, default_database, item, project=[i_wholesale_cost, i_class_id], metadata=[]]], fields=[i_wholesale_cost, i_class_id])
                     :                    +- Calc(select=[ws_sold_date_sk AS ws_sold_date_sk_node_12, ws_sold_time_sk AS ws_sold_time_sk_node_12, ws_ship_date_sk AS ws_ship_date_sk_node_12, ws_item_sk AS ws_item_sk_node_12, ws_bill_customer_sk AS ws_bill_customer_sk_node_12, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_12, ws_bill_addr_sk AS ws_bill_addr_sk_node_12, ws_ship_customer_sk AS ws_ship_customer_sk_node_12, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_12, ws_ship_addr_sk AS ws_ship_addr_sk_node_12, ws_web_page_sk AS ws_web_page_sk_node_12, ws_web_site_sk AS ws_web_site_sk_node_12, ws_ship_mode_sk AS ws_ship_mode_sk_node_12, ws_warehouse_sk AS ws_warehouse_sk_node_12, ws_promo_sk AS ws_promo_sk_node_12, ws_order_number AS ws_order_number_node_12, ws_quantity AS ws_quantity_node_12, ws_wholesale_cost AS ws_wholesale_cost_node_12, ws_list_price AS ws_list_price_node_12, ws_sales_price AS ws_sales_price_node_12, ws_ext_discount_amt AS ws_ext_discount_amt_node_12, ws_ext_sales_price AS ws_ext_sales_price_node_12, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_12, ws_ext_list_price AS ws_ext_list_price_node_12, ws_ext_tax AS ws_ext_tax_node_12, ws_coupon_amt AS ws_coupon_amt_node_12, ws_ext_ship_cost AS ws_ext_ship_cost_node_12, ws_net_paid AS ws_net_paid_node_12, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_12, ws_net_profit AS ws_net_profit_node_12, CAST(ws_ext_list_price AS DECIMAL(21, 2)) AS ws_ext_list_price_node_120])
                     :                       +- Sort(orderBy=[ws_list_price ASC])
                     :                          +- Exchange(distribution=[single])
                     :                             +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                     +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o318435964.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#642014687:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[20](input=RelSubset#642014685,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[20]), rel#642014684:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[20](input=RelSubset#642014683,groupBy=ws_bill_cdemo_sk_node_12, ws_ext_list_price_node_120,select=ws_bill_cdemo_sk_node_12, ws_ext_list_price_node_120, Partial_MIN(ws_order_number_node_12) AS min$0)]
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