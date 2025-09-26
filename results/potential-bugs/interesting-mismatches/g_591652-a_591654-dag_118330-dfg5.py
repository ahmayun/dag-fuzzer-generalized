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
    return values.quantile(0.75)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_9 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_8 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_7 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_6 = autonode_9.join(autonode_10, col('sr_store_sk_node_9') == col('p_item_sk_node_10'))
autonode_5 = autonode_7.join(autonode_8, col('ss_ext_tax_node_8') == col('cs_coupon_amt_node_7'))
autonode_4 = autonode_6.alias('TsI2r')
autonode_3 = autonode_5.order_by(col('ss_sales_price_node_8'))
autonode_2 = autonode_3.join(autonode_4, col('p_promo_sk_node_10') == col('cs_order_number_node_7'))
autonode_1 = autonode_2.group_by(col('cs_ship_mode_sk_node_7')).select(col('p_item_sk_node_10').min.alias('p_item_sk_node_10'))
sink = autonode_1.select(col('p_item_sk_node_10'))
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
LogicalProject(p_item_sk_node_10=[$1])
+- LogicalAggregate(group=[{13}], EXPR$0=[MIN($81)])
   +- LogicalJoin(condition=[=($77, $17)], joinType=[inner])
      :- LogicalSort(sort0=[$47], dir0=[ASC])
      :  +- LogicalJoin(condition=[=($52, $27)], joinType=[inner])
      :     :- LogicalProject(cs_sold_date_sk_node_7=[$0], cs_sold_time_sk_node_7=[$1], cs_ship_date_sk_node_7=[$2], cs_bill_customer_sk_node_7=[$3], cs_bill_cdemo_sk_node_7=[$4], cs_bill_hdemo_sk_node_7=[$5], cs_bill_addr_sk_node_7=[$6], cs_ship_customer_sk_node_7=[$7], cs_ship_cdemo_sk_node_7=[$8], cs_ship_hdemo_sk_node_7=[$9], cs_ship_addr_sk_node_7=[$10], cs_call_center_sk_node_7=[$11], cs_catalog_page_sk_node_7=[$12], cs_ship_mode_sk_node_7=[$13], cs_warehouse_sk_node_7=[$14], cs_item_sk_node_7=[$15], cs_promo_sk_node_7=[$16], cs_order_number_node_7=[$17], cs_quantity_node_7=[$18], cs_wholesale_cost_node_7=[$19], cs_list_price_node_7=[$20], cs_sales_price_node_7=[$21], cs_ext_discount_amt_node_7=[$22], cs_ext_sales_price_node_7=[$23], cs_ext_wholesale_cost_node_7=[$24], cs_ext_list_price_node_7=[$25], cs_ext_tax_node_7=[$26], cs_coupon_amt_node_7=[$27], cs_ext_ship_cost_node_7=[$28], cs_net_paid_node_7=[$29], cs_net_paid_inc_tax_node_7=[$30], cs_net_paid_inc_ship_node_7=[$31], cs_net_paid_inc_ship_tax_node_7=[$32], cs_net_profit_node_7=[$33])
      :     :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
      :     +- LogicalProject(ss_sold_date_sk_node_8=[$0], ss_sold_time_sk_node_8=[$1], ss_item_sk_node_8=[$2], ss_customer_sk_node_8=[$3], ss_cdemo_sk_node_8=[$4], ss_hdemo_sk_node_8=[$5], ss_addr_sk_node_8=[$6], ss_store_sk_node_8=[$7], ss_promo_sk_node_8=[$8], ss_ticket_number_node_8=[$9], ss_quantity_node_8=[$10], ss_wholesale_cost_node_8=[$11], ss_list_price_node_8=[$12], ss_sales_price_node_8=[$13], ss_ext_discount_amt_node_8=[$14], ss_ext_sales_price_node_8=[$15], ss_ext_wholesale_cost_node_8=[$16], ss_ext_list_price_node_8=[$17], ss_ext_tax_node_8=[$18], ss_coupon_amt_node_8=[$19], ss_net_paid_node_8=[$20], ss_net_paid_inc_tax_node_8=[$21], ss_net_profit_node_8=[$22])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalProject(TsI2r=[AS($0, _UTF-16LE'TsI2r')], sr_return_time_sk_node_9=[$1], sr_item_sk_node_9=[$2], sr_customer_sk_node_9=[$3], sr_cdemo_sk_node_9=[$4], sr_hdemo_sk_node_9=[$5], sr_addr_sk_node_9=[$6], sr_store_sk_node_9=[$7], sr_reason_sk_node_9=[$8], sr_ticket_number_node_9=[$9], sr_return_quantity_node_9=[$10], sr_return_amt_node_9=[$11], sr_return_tax_node_9=[$12], sr_return_amt_inc_tax_node_9=[$13], sr_fee_node_9=[$14], sr_return_ship_cost_node_9=[$15], sr_refunded_cash_node_9=[$16], sr_reversed_charge_node_9=[$17], sr_store_credit_node_9=[$18], sr_net_loss_node_9=[$19], p_promo_sk_node_10=[$20], p_promo_id_node_10=[$21], p_start_date_sk_node_10=[$22], p_end_date_sk_node_10=[$23], p_item_sk_node_10=[$24], p_cost_node_10=[$25], p_response_target_node_10=[$26], p_promo_name_node_10=[$27], p_channel_dmail_node_10=[$28], p_channel_email_node_10=[$29], p_channel_catalog_node_10=[$30], p_channel_tv_node_10=[$31], p_channel_radio_node_10=[$32], p_channel_press_node_10=[$33], p_channel_event_node_10=[$34], p_channel_demo_node_10=[$35], p_channel_details_node_10=[$36], p_purpose_node_10=[$37], p_discount_active_node_10=[$38])
         +- LogicalJoin(condition=[=($7, $24)], joinType=[inner])
            :- LogicalProject(sr_returned_date_sk_node_9=[$0], sr_return_time_sk_node_9=[$1], sr_item_sk_node_9=[$2], sr_customer_sk_node_9=[$3], sr_cdemo_sk_node_9=[$4], sr_hdemo_sk_node_9=[$5], sr_addr_sk_node_9=[$6], sr_store_sk_node_9=[$7], sr_reason_sk_node_9=[$8], sr_ticket_number_node_9=[$9], sr_return_quantity_node_9=[$10], sr_return_amt_node_9=[$11], sr_return_tax_node_9=[$12], sr_return_amt_inc_tax_node_9=[$13], sr_fee_node_9=[$14], sr_return_ship_cost_node_9=[$15], sr_refunded_cash_node_9=[$16], sr_reversed_charge_node_9=[$17], sr_store_credit_node_9=[$18], sr_net_loss_node_9=[$19])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
            +- LogicalProject(p_promo_sk_node_10=[$0], p_promo_id_node_10=[$1], p_start_date_sk_node_10=[$2], p_end_date_sk_node_10=[$3], p_item_sk_node_10=[$4], p_cost_node_10=[$5], p_response_target_node_10=[$6], p_promo_name_node_10=[$7], p_channel_dmail_node_10=[$8], p_channel_email_node_10=[$9], p_channel_catalog_node_10=[$10], p_channel_tv_node_10=[$11], p_channel_radio_node_10=[$12], p_channel_press_node_10=[$13], p_channel_event_node_10=[$14], p_channel_demo_node_10=[$15], p_channel_details_node_10=[$16], p_purpose_node_10=[$17], p_discount_active_node_10=[$18])
               +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS p_item_sk_node_10])
+- HashAggregate(isMerge=[true], groupBy=[cs_ship_mode_sk], select=[cs_ship_mode_sk, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cs_ship_mode_sk]])
      +- LocalHashAggregate(groupBy=[cs_ship_mode_sk], select=[cs_ship_mode_sk, Partial_MIN(p_item_sk_node_10) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(p_promo_sk_node_10, cs_order_number)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, TsI2r, sr_return_time_sk_node_9, sr_item_sk_node_9, sr_customer_sk_node_9, sr_cdemo_sk_node_9, sr_hdemo_sk_node_9, sr_addr_sk_node_9, sr_store_sk_node_9, sr_reason_sk_node_9, sr_ticket_number_node_9, sr_return_quantity_node_9, sr_return_amt_node_9, sr_return_tax_node_9, sr_return_amt_inc_tax_node_9, sr_fee_node_9, sr_return_ship_cost_node_9, sr_refunded_cash_node_9, sr_reversed_charge_node_9, sr_store_credit_node_9, sr_net_loss_node_9, p_promo_sk_node_10, p_promo_id_node_10, p_start_date_sk_node_10, p_end_date_sk_node_10, p_item_sk_node_10, p_cost_node_10, p_response_target_node_10, p_promo_name_node_10, p_channel_dmail_node_10, p_channel_email_node_10, p_channel_catalog_node_10, p_channel_tv_node_10, p_channel_radio_node_10, p_channel_press_node_10, p_channel_event_node_10, p_channel_demo_node_10, p_channel_details_node_10, p_purpose_node_10, p_discount_active_node_10], build=[right])
            :- Sort(orderBy=[ss_sales_price ASC])
            :  +- Exchange(distribution=[single])
            :     +- HashJoin(joinType=[InnerJoin], where=[=(ss_ext_tax, cs_coupon_amt)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
            :        :- Exchange(distribution=[hash[cs_coupon_amt]])
            :        :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
            :        +- Exchange(distribution=[hash[ss_ext_tax]])
            :           +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[sr_returned_date_sk AS TsI2r, sr_return_time_sk AS sr_return_time_sk_node_9, sr_item_sk AS sr_item_sk_node_9, sr_customer_sk AS sr_customer_sk_node_9, sr_cdemo_sk AS sr_cdemo_sk_node_9, sr_hdemo_sk AS sr_hdemo_sk_node_9, sr_addr_sk AS sr_addr_sk_node_9, sr_store_sk AS sr_store_sk_node_9, sr_reason_sk AS sr_reason_sk_node_9, sr_ticket_number AS sr_ticket_number_node_9, sr_return_quantity AS sr_return_quantity_node_9, sr_return_amt AS sr_return_amt_node_9, sr_return_tax AS sr_return_tax_node_9, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_9, sr_fee AS sr_fee_node_9, sr_return_ship_cost AS sr_return_ship_cost_node_9, sr_refunded_cash AS sr_refunded_cash_node_9, sr_reversed_charge AS sr_reversed_charge_node_9, sr_store_credit AS sr_store_credit_node_9, sr_net_loss AS sr_net_loss_node_9, p_promo_sk AS p_promo_sk_node_10, p_promo_id AS p_promo_id_node_10, p_start_date_sk AS p_start_date_sk_node_10, p_end_date_sk AS p_end_date_sk_node_10, p_item_sk AS p_item_sk_node_10, p_cost AS p_cost_node_10, p_response_target AS p_response_target_node_10, p_promo_name AS p_promo_name_node_10, p_channel_dmail AS p_channel_dmail_node_10, p_channel_email AS p_channel_email_node_10, p_channel_catalog AS p_channel_catalog_node_10, p_channel_tv AS p_channel_tv_node_10, p_channel_radio AS p_channel_radio_node_10, p_channel_press AS p_channel_press_node_10, p_channel_event AS p_channel_event_node_10, p_channel_demo AS p_channel_demo_node_10, p_channel_details AS p_channel_details_node_10, p_purpose AS p_purpose_node_10, p_discount_active AS p_discount_active_node_10])
                  +- HashJoin(joinType=[InnerJoin], where=[=(sr_store_sk, p_item_sk)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], isBroadcast=[true], build=[right])
                     :- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                     +- Exchange(distribution=[broadcast])
                        +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS p_item_sk_node_10])
+- HashAggregate(isMerge=[true], groupBy=[cs_ship_mode_sk], select=[cs_ship_mode_sk, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cs_ship_mode_sk]])
      +- LocalHashAggregate(groupBy=[cs_ship_mode_sk], select=[cs_ship_mode_sk, Partial_MIN(p_item_sk_node_10) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(p_promo_sk_node_10 = cs_order_number)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, TsI2r, sr_return_time_sk_node_9, sr_item_sk_node_9, sr_customer_sk_node_9, sr_cdemo_sk_node_9, sr_hdemo_sk_node_9, sr_addr_sk_node_9, sr_store_sk_node_9, sr_reason_sk_node_9, sr_ticket_number_node_9, sr_return_quantity_node_9, sr_return_amt_node_9, sr_return_tax_node_9, sr_return_amt_inc_tax_node_9, sr_fee_node_9, sr_return_ship_cost_node_9, sr_refunded_cash_node_9, sr_reversed_charge_node_9, sr_store_credit_node_9, sr_net_loss_node_9, p_promo_sk_node_10, p_promo_id_node_10, p_start_date_sk_node_10, p_end_date_sk_node_10, p_item_sk_node_10, p_cost_node_10, p_response_target_node_10, p_promo_name_node_10, p_channel_dmail_node_10, p_channel_email_node_10, p_channel_catalog_node_10, p_channel_tv_node_10, p_channel_radio_node_10, p_channel_press_node_10, p_channel_event_node_10, p_channel_demo_node_10, p_channel_details_node_10, p_purpose_node_10, p_discount_active_node_10], build=[right])
            :- Sort(orderBy=[ss_sales_price ASC])
            :  +- Exchange(distribution=[single])
            :     +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ss_ext_tax = cs_coupon_amt)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
            :        :- Exchange(distribution=[hash[cs_coupon_amt]])
            :        :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
            :        +- Exchange(distribution=[hash[ss_ext_tax]])
            :           +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[sr_returned_date_sk AS TsI2r, sr_return_time_sk AS sr_return_time_sk_node_9, sr_item_sk AS sr_item_sk_node_9, sr_customer_sk AS sr_customer_sk_node_9, sr_cdemo_sk AS sr_cdemo_sk_node_9, sr_hdemo_sk AS sr_hdemo_sk_node_9, sr_addr_sk AS sr_addr_sk_node_9, sr_store_sk AS sr_store_sk_node_9, sr_reason_sk AS sr_reason_sk_node_9, sr_ticket_number AS sr_ticket_number_node_9, sr_return_quantity AS sr_return_quantity_node_9, sr_return_amt AS sr_return_amt_node_9, sr_return_tax AS sr_return_tax_node_9, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_9, sr_fee AS sr_fee_node_9, sr_return_ship_cost AS sr_return_ship_cost_node_9, sr_refunded_cash AS sr_refunded_cash_node_9, sr_reversed_charge AS sr_reversed_charge_node_9, sr_store_credit AS sr_store_credit_node_9, sr_net_loss AS sr_net_loss_node_9, p_promo_sk AS p_promo_sk_node_10, p_promo_id AS p_promo_id_node_10, p_start_date_sk AS p_start_date_sk_node_10, p_end_date_sk AS p_end_date_sk_node_10, p_item_sk AS p_item_sk_node_10, p_cost AS p_cost_node_10, p_response_target AS p_response_target_node_10, p_promo_name AS p_promo_name_node_10, p_channel_dmail AS p_channel_dmail_node_10, p_channel_email AS p_channel_email_node_10, p_channel_catalog AS p_channel_catalog_node_10, p_channel_tv AS p_channel_tv_node_10, p_channel_radio AS p_channel_radio_node_10, p_channel_press AS p_channel_press_node_10, p_channel_event AS p_channel_event_node_10, p_channel_demo AS p_channel_demo_node_10, p_channel_details AS p_channel_details_node_10, p_purpose AS p_purpose_node_10, p_discount_active AS p_discount_active_node_10])
                  +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(sr_store_sk = p_item_sk)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])\
+- [#2] Exchange(distribution=[broadcast])\
])
                     :- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                     +- Exchange(distribution=[broadcast])
                        +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o322709602.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#650847963:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[47](input=RelSubset#650847961,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[47]), rel#650847960:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[47](input=RelSubset#650847959,groupBy=cs_ship_mode_sk, cs_order_number,select=cs_ship_mode_sk, cs_order_number)]
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