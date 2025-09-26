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
    return values.iloc[-1] if len(values) > 0 else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_9 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_12 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_11 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_6 = autonode_10.alias('DPAbY')
autonode_5 = autonode_9.order_by(col('ws_quantity_node_9'))
autonode_8 = autonode_12.add_columns(lit("hello"))
autonode_7 = autonode_11.limit(41)
autonode_3 = autonode_5.join(autonode_6, col('ws_ship_cdemo_sk_node_9') == col('cr_returning_addr_sk_node_10'))
autonode_4 = autonode_7.join(autonode_8, col('c_first_sales_date_sk_node_11') == col('p_promo_sk_node_12'))
autonode_1 = autonode_3.group_by(col('ws_net_paid_inc_tax_node_9')).select(col('ws_net_paid_inc_ship_tax_node_9').min.alias('ws_net_paid_inc_ship_tax_node_9'))
autonode_2 = autonode_4.order_by(col('p_channel_tv_node_12'))
sink = autonode_1.join(autonode_2, col('p_cost_node_12') == col('ws_net_paid_inc_ship_tax_node_9'))
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
LogicalJoin(condition=[=($24, $0)], joinType=[inner])
:- LogicalProject(ws_net_paid_inc_ship_tax_node_9=[$1])
:  +- LogicalAggregate(group=[{30}], EXPR$0=[MIN($32)])
:     +- LogicalJoin(condition=[=($9, $44)], joinType=[inner])
:        :- LogicalSort(sort0=[$18], dir0=[ASC])
:        :  +- LogicalProject(ws_sold_date_sk_node_9=[$0], ws_sold_time_sk_node_9=[$1], ws_ship_date_sk_node_9=[$2], ws_item_sk_node_9=[$3], ws_bill_customer_sk_node_9=[$4], ws_bill_cdemo_sk_node_9=[$5], ws_bill_hdemo_sk_node_9=[$6], ws_bill_addr_sk_node_9=[$7], ws_ship_customer_sk_node_9=[$8], ws_ship_cdemo_sk_node_9=[$9], ws_ship_hdemo_sk_node_9=[$10], ws_ship_addr_sk_node_9=[$11], ws_web_page_sk_node_9=[$12], ws_web_site_sk_node_9=[$13], ws_ship_mode_sk_node_9=[$14], ws_warehouse_sk_node_9=[$15], ws_promo_sk_node_9=[$16], ws_order_number_node_9=[$17], ws_quantity_node_9=[$18], ws_wholesale_cost_node_9=[$19], ws_list_price_node_9=[$20], ws_sales_price_node_9=[$21], ws_ext_discount_amt_node_9=[$22], ws_ext_sales_price_node_9=[$23], ws_ext_wholesale_cost_node_9=[$24], ws_ext_list_price_node_9=[$25], ws_ext_tax_node_9=[$26], ws_coupon_amt_node_9=[$27], ws_ext_ship_cost_node_9=[$28], ws_net_paid_node_9=[$29], ws_net_paid_inc_tax_node_9=[$30], ws_net_paid_inc_ship_node_9=[$31], ws_net_paid_inc_ship_tax_node_9=[$32], ws_net_profit_node_9=[$33])
:        :     +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
:        +- LogicalProject(DPAbY=[AS($0, _UTF-16LE'DPAbY')], cr_returned_time_sk_node_10=[$1], cr_item_sk_node_10=[$2], cr_refunded_customer_sk_node_10=[$3], cr_refunded_cdemo_sk_node_10=[$4], cr_refunded_hdemo_sk_node_10=[$5], cr_refunded_addr_sk_node_10=[$6], cr_returning_customer_sk_node_10=[$7], cr_returning_cdemo_sk_node_10=[$8], cr_returning_hdemo_sk_node_10=[$9], cr_returning_addr_sk_node_10=[$10], cr_call_center_sk_node_10=[$11], cr_catalog_page_sk_node_10=[$12], cr_ship_mode_sk_node_10=[$13], cr_warehouse_sk_node_10=[$14], cr_reason_sk_node_10=[$15], cr_order_number_node_10=[$16], cr_return_quantity_node_10=[$17], cr_return_amount_node_10=[$18], cr_return_tax_node_10=[$19], cr_return_amt_inc_tax_node_10=[$20], cr_fee_node_10=[$21], cr_return_ship_cost_node_10=[$22], cr_refunded_cash_node_10=[$23], cr_reversed_charge_node_10=[$24], cr_store_credit_node_10=[$25], cr_net_loss_node_10=[$26])
:           +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
+- LogicalSort(sort0=[$29], dir0=[ASC])
   +- LogicalJoin(condition=[=($6, $18)], joinType=[inner])
      :- LogicalSort(fetch=[41])
      :  +- LogicalProject(c_customer_sk_node_11=[$0], c_customer_id_node_11=[$1], c_current_cdemo_sk_node_11=[$2], c_current_hdemo_sk_node_11=[$3], c_current_addr_sk_node_11=[$4], c_first_shipto_date_sk_node_11=[$5], c_first_sales_date_sk_node_11=[$6], c_salutation_node_11=[$7], c_first_name_node_11=[$8], c_last_name_node_11=[$9], c_preferred_cust_flag_node_11=[$10], c_birth_day_node_11=[$11], c_birth_month_node_11=[$12], c_birth_year_node_11=[$13], c_birth_country_node_11=[$14], c_login_node_11=[$15], c_email_address_node_11=[$16], c_last_review_date_node_11=[$17])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
      +- LogicalProject(p_promo_sk_node_12=[$0], p_promo_id_node_12=[$1], p_start_date_sk_node_12=[$2], p_end_date_sk_node_12=[$3], p_item_sk_node_12=[$4], p_cost_node_12=[$5], p_response_target_node_12=[$6], p_promo_name_node_12=[$7], p_channel_dmail_node_12=[$8], p_channel_email_node_12=[$9], p_channel_catalog_node_12=[$10], p_channel_tv_node_12=[$11], p_channel_radio_node_12=[$12], p_channel_press_node_12=[$13], p_channel_event_node_12=[$14], p_channel_demo_node_12=[$15], p_channel_details_node_12=[$16], p_purpose_node_12=[$17], p_discount_active_node_12=[$18], _c19=[_UTF-16LE'hello'])
         +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])

== Optimized Physical Plan ==
Calc(select=[ws_net_paid_inc_ship_tax_node_9, c_customer_sk_node_11, c_customer_id_node_11, c_current_cdemo_sk_node_11, c_current_hdemo_sk_node_11, c_current_addr_sk_node_11, c_first_shipto_date_sk_node_11, c_first_sales_date_sk_node_11, c_salutation_node_11, c_first_name_node_11, c_last_name_node_11, c_preferred_cust_flag_node_11, c_birth_day_node_11, c_birth_month_node_11, c_birth_year_node_11, c_birth_country_node_11, c_login_node_11, c_email_address_node_11, c_last_review_date_node_11, p_promo_sk_node_12, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12, 'hello' AS _c19])
+- HashJoin(joinType=[InnerJoin], where=[=(p_cost_node_12, ws_net_paid_inc_ship_tax_node_90)], select=[ws_net_paid_inc_ship_tax_node_9, ws_net_paid_inc_ship_tax_node_90, c_customer_sk_node_11, c_customer_id_node_11, c_current_cdemo_sk_node_11, c_current_hdemo_sk_node_11, c_current_addr_sk_node_11, c_first_shipto_date_sk_node_11, c_first_sales_date_sk_node_11, c_salutation_node_11, c_first_name_node_11, c_last_name_node_11, c_preferred_cust_flag_node_11, c_birth_day_node_11, c_birth_month_node_11, c_birth_year_node_11, c_birth_country_node_11, c_login_node_11, c_email_address_node_11, c_last_review_date_node_11, p_promo_sk_node_12, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12], isBroadcast=[true], build=[right])
   :- Calc(select=[EXPR$0 AS ws_net_paid_inc_ship_tax_node_9, CAST(EXPR$0 AS DECIMAL(15, 2)) AS ws_net_paid_inc_ship_tax_node_90])
   :  +- HashAggregate(isMerge=[false], groupBy=[ws_net_paid_inc_tax], select=[ws_net_paid_inc_tax, MIN(ws_net_paid_inc_ship_tax) AS EXPR$0])
   :     +- Exchange(distribution=[hash[ws_net_paid_inc_tax]])
   :        +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ws_ship_cdemo_sk, cr_returning_addr_sk_node_10)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, DPAbY, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10], build=[right])
   :           :- Sort(orderBy=[ws_quantity ASC])
   :           :  +- Exchange(distribution=[single])
   :           :     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
   :           +- Exchange(distribution=[broadcast])
   :              +- Calc(select=[cr_returned_date_sk AS DPAbY, cr_returned_time_sk AS cr_returned_time_sk_node_10, cr_item_sk AS cr_item_sk_node_10, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_10, cr_returning_customer_sk AS cr_returning_customer_sk_node_10, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_10, cr_returning_addr_sk AS cr_returning_addr_sk_node_10, cr_call_center_sk AS cr_call_center_sk_node_10, cr_catalog_page_sk AS cr_catalog_page_sk_node_10, cr_ship_mode_sk AS cr_ship_mode_sk_node_10, cr_warehouse_sk AS cr_warehouse_sk_node_10, cr_reason_sk AS cr_reason_sk_node_10, cr_order_number AS cr_order_number_node_10, cr_return_quantity AS cr_return_quantity_node_10, cr_return_amount AS cr_return_amount_node_10, cr_return_tax AS cr_return_tax_node_10, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_10, cr_fee AS cr_fee_node_10, cr_return_ship_cost AS cr_return_ship_cost_node_10, cr_refunded_cash AS cr_refunded_cash_node_10, cr_reversed_charge AS cr_reversed_charge_node_10, cr_store_credit AS cr_store_credit_node_10, cr_net_loss AS cr_net_loss_node_10])
   :                 +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
   +- Exchange(distribution=[broadcast])
      +- Calc(select=[c_customer_sk AS c_customer_sk_node_11, c_customer_id AS c_customer_id_node_11, c_current_cdemo_sk AS c_current_cdemo_sk_node_11, c_current_hdemo_sk AS c_current_hdemo_sk_node_11, c_current_addr_sk AS c_current_addr_sk_node_11, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_11, c_first_sales_date_sk AS c_first_sales_date_sk_node_11, c_salutation AS c_salutation_node_11, c_first_name AS c_first_name_node_11, c_last_name AS c_last_name_node_11, c_preferred_cust_flag AS c_preferred_cust_flag_node_11, c_birth_day AS c_birth_day_node_11, c_birth_month AS c_birth_month_node_11, c_birth_year AS c_birth_year_node_11, c_birth_country AS c_birth_country_node_11, c_login AS c_login_node_11, c_email_address AS c_email_address_node_11, c_last_review_date AS c_last_review_date_node_11, p_promo_sk_node_12, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12])
         +- Sort(orderBy=[p_channel_tv_node_12 ASC])
            +- Exchange(distribution=[single])
               +- HashJoin(joinType=[InnerJoin], where=[=(c_first_sales_date_sk, p_promo_sk_node_12)], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, p_promo_sk_node_12, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12, _c19], isBroadcast=[true], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Limit(offset=[0], fetch=[41], global=[true])
                  :     +- Exchange(distribution=[single])
                  :        +- Limit(offset=[0], fetch=[41], global=[false])
                  :           +- TableSourceScan(table=[[default_catalog, default_database, customer, limit=[41]]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                  +- Calc(select=[p_promo_sk AS p_promo_sk_node_12, p_promo_id AS p_promo_id_node_12, p_start_date_sk AS p_start_date_sk_node_12, p_end_date_sk AS p_end_date_sk_node_12, p_item_sk AS p_item_sk_node_12, p_cost AS p_cost_node_12, p_response_target AS p_response_target_node_12, p_promo_name AS p_promo_name_node_12, p_channel_dmail AS p_channel_dmail_node_12, p_channel_email AS p_channel_email_node_12, p_channel_catalog AS p_channel_catalog_node_12, p_channel_tv AS p_channel_tv_node_12, p_channel_radio AS p_channel_radio_node_12, p_channel_press AS p_channel_press_node_12, p_channel_event AS p_channel_event_node_12, p_channel_demo AS p_channel_demo_node_12, p_channel_details AS p_channel_details_node_12, p_purpose AS p_purpose_node_12, p_discount_active AS p_discount_active_node_12, 'hello' AS _c19])
                     +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

== Optimized Execution Plan ==
Calc(select=[ws_net_paid_inc_ship_tax_node_9, c_customer_sk_node_11, c_customer_id_node_11, c_current_cdemo_sk_node_11, c_current_hdemo_sk_node_11, c_current_addr_sk_node_11, c_first_shipto_date_sk_node_11, c_first_sales_date_sk_node_11, c_salutation_node_11, c_first_name_node_11, c_last_name_node_11, c_preferred_cust_flag_node_11, c_birth_day_node_11, c_birth_month_node_11, c_birth_year_node_11, c_birth_country_node_11, c_login_node_11, c_email_address_node_11, c_last_review_date_node_11, p_promo_sk_node_12, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12, 'hello' AS _c19])
+- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(p_cost_node_12 = ws_net_paid_inc_ship_tax_node_90)], select=[ws_net_paid_inc_ship_tax_node_9, ws_net_paid_inc_ship_tax_node_90, c_customer_sk_node_11, c_customer_id_node_11, c_current_cdemo_sk_node_11, c_current_hdemo_sk_node_11, c_current_addr_sk_node_11, c_first_shipto_date_sk_node_11, c_first_sales_date_sk_node_11, c_salutation_node_11, c_first_name_node_11, c_last_name_node_11, c_preferred_cust_flag_node_11, c_birth_day_node_11, c_birth_month_node_11, c_birth_year_node_11, c_birth_country_node_11, c_login_node_11, c_email_address_node_11, c_last_review_date_node_11, p_promo_sk_node_12, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12], isBroadcast=[true], build=[right])\
:- Calc(select=[EXPR$0 AS ws_net_paid_inc_ship_tax_node_9, CAST(EXPR$0 AS DECIMAL(15, 2)) AS ws_net_paid_inc_ship_tax_node_90])\
:  +- HashAggregate(isMerge=[false], groupBy=[ws_net_paid_inc_tax], select=[ws_net_paid_inc_tax, MIN(ws_net_paid_inc_ship_tax) AS EXPR$0])\
:     +- [#2] Exchange(distribution=[hash[ws_net_paid_inc_tax]])\
+- [#1] Exchange(distribution=[broadcast])\
])
   :- Exchange(distribution=[broadcast])
   :  +- Calc(select=[c_customer_sk AS c_customer_sk_node_11, c_customer_id AS c_customer_id_node_11, c_current_cdemo_sk AS c_current_cdemo_sk_node_11, c_current_hdemo_sk AS c_current_hdemo_sk_node_11, c_current_addr_sk AS c_current_addr_sk_node_11, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_11, c_first_sales_date_sk AS c_first_sales_date_sk_node_11, c_salutation AS c_salutation_node_11, c_first_name AS c_first_name_node_11, c_last_name AS c_last_name_node_11, c_preferred_cust_flag AS c_preferred_cust_flag_node_11, c_birth_day AS c_birth_day_node_11, c_birth_month AS c_birth_month_node_11, c_birth_year AS c_birth_year_node_11, c_birth_country AS c_birth_country_node_11, c_login AS c_login_node_11, c_email_address AS c_email_address_node_11, c_last_review_date AS c_last_review_date_node_11, p_promo_sk_node_12, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12])
   :     +- Sort(orderBy=[p_channel_tv_node_12 ASC])
   :        +- Exchange(distribution=[single])
   :           +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(c_first_sales_date_sk = p_promo_sk_node_12)], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, p_promo_sk_node_12, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12, _c19], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- Calc(select=[p_promo_sk AS p_promo_sk_node_12, p_promo_id AS p_promo_id_node_12, p_start_date_sk AS p_start_date_sk_node_12, p_end_date_sk AS p_end_date_sk_node_12, p_item_sk AS p_item_sk_node_12, p_cost AS p_cost_node_12, p_response_target AS p_response_target_node_12, p_promo_name AS p_promo_name_node_12, p_channel_dmail AS p_channel_dmail_node_12, p_channel_email AS p_channel_email_node_12, p_channel_catalog AS p_channel_catalog_node_12, p_channel_tv AS p_channel_tv_node_12, p_channel_radio AS p_channel_radio_node_12, p_channel_press AS p_channel_press_node_12, p_channel_event AS p_channel_event_node_12, p_channel_demo AS p_channel_demo_node_12, p_channel_details AS p_channel_details_node_12, p_purpose AS p_purpose_node_12, p_discount_active AS p_discount_active_node_12, 'hello' AS _c19])\
   +- [#2] TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])\
])
   :              :- Exchange(distribution=[broadcast])
   :              :  +- Limit(offset=[0], fetch=[41], global=[true])
   :              :     +- Exchange(distribution=[single])
   :              :        +- Limit(offset=[0], fetch=[41], global=[false])
   :              :           +- TableSourceScan(table=[[default_catalog, default_database, customer, limit=[41]]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
   :              +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
   +- Exchange(distribution=[hash[ws_net_paid_inc_tax]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[(ws_ship_cdemo_sk = cr_returning_addr_sk_node_10)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, DPAbY, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10], build=[right])
         :- Sort(orderBy=[ws_quantity ASC])
         :  +- Exchange(distribution=[single])
         :     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[cr_returned_date_sk AS DPAbY, cr_returned_time_sk AS cr_returned_time_sk_node_10, cr_item_sk AS cr_item_sk_node_10, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_10, cr_returning_customer_sk AS cr_returning_customer_sk_node_10, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_10, cr_returning_addr_sk AS cr_returning_addr_sk_node_10, cr_call_center_sk AS cr_call_center_sk_node_10, cr_catalog_page_sk AS cr_catalog_page_sk_node_10, cr_ship_mode_sk AS cr_ship_mode_sk_node_10, cr_warehouse_sk AS cr_warehouse_sk_node_10, cr_reason_sk AS cr_reason_sk_node_10, cr_order_number AS cr_order_number_node_10, cr_return_quantity AS cr_return_quantity_node_10, cr_return_amount AS cr_return_amount_node_10, cr_return_tax AS cr_return_tax_node_10, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_10, cr_fee AS cr_fee_node_10, cr_return_ship_cost AS cr_return_ship_cost_node_10, cr_refunded_cash AS cr_refunded_cash_node_10, cr_reversed_charge AS cr_reversed_charge_node_10, cr_store_credit AS cr_store_credit_node_10, cr_net_loss AS cr_net_loss_node_10])
               +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o255742666.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#517016933:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[18](input=RelSubset#517016931,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[18]), rel#517016930:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[18](input=RelSubset#517016929,groupBy=ws_ship_cdemo_sk, ws_net_paid_inc_tax,select=ws_ship_cdemo_sk, ws_net_paid_inc_tax, Partial_MIN(ws_net_paid_inc_ship_tax) AS min$0)]
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