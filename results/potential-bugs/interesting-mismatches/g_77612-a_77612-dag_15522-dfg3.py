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
    return values.product()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_12 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_11 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_7 = autonode_10.order_by(col('cs_item_sk_node_10'))
autonode_9 = autonode_12.alias('3pPo4')
autonode_8 = autonode_11.alias('NfRkv')
autonode_6 = autonode_9.distinct()
autonode_5 = autonode_7.join(autonode_8, col('p_item_sk_node_11') == col('cs_ship_customer_sk_node_10'))
autonode_4 = autonode_6.order_by(col('p_item_sk_node_12'))
autonode_3 = autonode_5.group_by(col('cs_net_paid_inc_ship_node_10')).select(col('p_channel_radio_node_11').min.alias('p_channel_radio_node_11'))
autonode_2 = autonode_3.join(autonode_4, col('p_channel_radio_node_11') == col('p_promo_name_node_12'))
autonode_1 = autonode_2.alias('fNb8l')
sink = autonode_1.group_by(col('p_purpose_node_12')).select(col('p_channel_catalog_node_12').max.alias('p_channel_catalog_node_12'))
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
LogicalProject(p_channel_catalog_node_12=[$1])
+- LogicalAggregate(group=[{1}], EXPR$0=[MAX($0)])
   +- LogicalProject(p_channel_catalog_node_12=[$11], p_purpose_node_12=[$18])
      +- LogicalJoin(condition=[=($0, $8)], joinType=[inner])
         :- LogicalProject(p_channel_radio_node_11=[$1])
         :  +- LogicalAggregate(group=[{31}], EXPR$0=[MIN($46)])
         :     +- LogicalJoin(condition=[=($38, $7)], joinType=[inner])
         :        :- LogicalSort(sort0=[$15], dir0=[ASC])
         :        :  +- LogicalProject(cs_sold_date_sk_node_10=[$0], cs_sold_time_sk_node_10=[$1], cs_ship_date_sk_node_10=[$2], cs_bill_customer_sk_node_10=[$3], cs_bill_cdemo_sk_node_10=[$4], cs_bill_hdemo_sk_node_10=[$5], cs_bill_addr_sk_node_10=[$6], cs_ship_customer_sk_node_10=[$7], cs_ship_cdemo_sk_node_10=[$8], cs_ship_hdemo_sk_node_10=[$9], cs_ship_addr_sk_node_10=[$10], cs_call_center_sk_node_10=[$11], cs_catalog_page_sk_node_10=[$12], cs_ship_mode_sk_node_10=[$13], cs_warehouse_sk_node_10=[$14], cs_item_sk_node_10=[$15], cs_promo_sk_node_10=[$16], cs_order_number_node_10=[$17], cs_quantity_node_10=[$18], cs_wholesale_cost_node_10=[$19], cs_list_price_node_10=[$20], cs_sales_price_node_10=[$21], cs_ext_discount_amt_node_10=[$22], cs_ext_sales_price_node_10=[$23], cs_ext_wholesale_cost_node_10=[$24], cs_ext_list_price_node_10=[$25], cs_ext_tax_node_10=[$26], cs_coupon_amt_node_10=[$27], cs_ext_ship_cost_node_10=[$28], cs_net_paid_node_10=[$29], cs_net_paid_inc_tax_node_10=[$30], cs_net_paid_inc_ship_node_10=[$31], cs_net_paid_inc_ship_tax_node_10=[$32], cs_net_profit_node_10=[$33])
         :        :     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
         :        +- LogicalProject(NfRkv=[AS($0, _UTF-16LE'NfRkv')], p_promo_id_node_11=[$1], p_start_date_sk_node_11=[$2], p_end_date_sk_node_11=[$3], p_item_sk_node_11=[$4], p_cost_node_11=[$5], p_response_target_node_11=[$6], p_promo_name_node_11=[$7], p_channel_dmail_node_11=[$8], p_channel_email_node_11=[$9], p_channel_catalog_node_11=[$10], p_channel_tv_node_11=[$11], p_channel_radio_node_11=[$12], p_channel_press_node_11=[$13], p_channel_event_node_11=[$14], p_channel_demo_node_11=[$15], p_channel_details_node_11=[$16], p_purpose_node_11=[$17], p_discount_active_node_11=[$18])
         :           +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])
         +- LogicalSort(sort0=[$4], dir0=[ASC])
            +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}])
               +- LogicalProject(3pPo4=[AS($0, _UTF-16LE'3pPo4')], p_promo_id_node_12=[$1], p_start_date_sk_node_12=[$2], p_end_date_sk_node_12=[$3], p_item_sk_node_12=[$4], p_cost_node_12=[$5], p_response_target_node_12=[$6], p_promo_name_node_12=[$7], p_channel_dmail_node_12=[$8], p_channel_email_node_12=[$9], p_channel_catalog_node_12=[$10], p_channel_tv_node_12=[$11], p_channel_radio_node_12=[$12], p_channel_press_node_12=[$13], p_channel_event_node_12=[$14], p_channel_demo_node_12=[$15], p_channel_details_node_12=[$16], p_purpose_node_12=[$17], p_discount_active_node_12=[$18])
                  +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS p_channel_catalog_node_12])
+- SortAggregate(isMerge=[true], groupBy=[p_purpose_node_12], select=[p_purpose_node_12, Final_MAX(max$0) AS EXPR$0])
   +- Sort(orderBy=[p_purpose_node_12 ASC])
      +- Exchange(distribution=[hash[p_purpose_node_12]])
         +- LocalSortAggregate(groupBy=[p_purpose_node_12], select=[p_purpose_node_12, Partial_MAX(EXPR$0) AS max$0])
            +- Sort(orderBy=[p_purpose_node_12 ASC])
               +- HashJoin(joinType=[InnerJoin], where=[=(p_channel_radio_node_11, p_promo_name_node_12)], select=[p_channel_radio_node_11, p_promo_name_node_12, p_purpose_node_12, EXPR$0], isBroadcast=[true], build=[right])
                  :- HashAggregate(isMerge=[true], groupBy=[p_channel_radio_node_11], select=[p_channel_radio_node_11])
                  :  +- Exchange(distribution=[hash[p_channel_radio_node_11]])
                  :     +- LocalHashAggregate(groupBy=[p_channel_radio_node_11], select=[p_channel_radio_node_11])
                  :        +- Calc(select=[EXPR$0 AS p_channel_radio_node_11])
                  :           +- SortAggregate(isMerge=[false], groupBy=[cs_net_paid_inc_ship], select=[cs_net_paid_inc_ship, MIN(p_channel_radio_node_11) AS EXPR$0])
                  :              +- Sort(orderBy=[cs_net_paid_inc_ship ASC])
                  :                 +- Exchange(distribution=[hash[cs_net_paid_inc_ship]])
                  :                    +- HashJoin(joinType=[InnerJoin], where=[=(p_item_sk_node_11, cs_ship_customer_sk)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, NfRkv, p_promo_id_node_11, p_start_date_sk_node_11, p_end_date_sk_node_11, p_item_sk_node_11, p_cost_node_11, p_response_target_node_11, p_promo_name_node_11, p_channel_dmail_node_11, p_channel_email_node_11, p_channel_catalog_node_11, p_channel_tv_node_11, p_channel_radio_node_11, p_channel_press_node_11, p_channel_event_node_11, p_channel_demo_node_11, p_channel_details_node_11, p_purpose_node_11, p_discount_active_node_11], isBroadcast=[true], build=[right])
                  :                       :- Sort(orderBy=[cs_item_sk ASC])
                  :                       :  +- Exchange(distribution=[single])
                  :                       :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
                  :                       +- Exchange(distribution=[broadcast])
                  :                          +- Calc(select=[p_promo_sk AS NfRkv, p_promo_id AS p_promo_id_node_11, p_start_date_sk AS p_start_date_sk_node_11, p_end_date_sk AS p_end_date_sk_node_11, p_item_sk AS p_item_sk_node_11, p_cost AS p_cost_node_11, p_response_target AS p_response_target_node_11, p_promo_name AS p_promo_name_node_11, p_channel_dmail AS p_channel_dmail_node_11, p_channel_email AS p_channel_email_node_11, p_channel_catalog AS p_channel_catalog_node_11, p_channel_tv AS p_channel_tv_node_11, p_channel_radio AS p_channel_radio_node_11, p_channel_press AS p_channel_press_node_11, p_channel_event AS p_channel_event_node_11, p_channel_demo AS p_channel_demo_node_11, p_channel_details AS p_channel_details_node_11, p_purpose AS p_purpose_node_11, p_discount_active AS p_discount_active_node_11])
                  :                             +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                  +- Exchange(distribution=[broadcast])
                     +- SortAggregate(isMerge=[true], groupBy=[p_promo_name_node_12, p_purpose_node_12], select=[p_promo_name_node_12, p_purpose_node_12, Final_MAX(max$0) AS EXPR$0])
                        +- Sort(orderBy=[p_promo_name_node_12 ASC, p_purpose_node_12 ASC])
                           +- Exchange(distribution=[hash[p_promo_name_node_12, p_purpose_node_12]])
                              +- LocalSortAggregate(groupBy=[p_promo_name_node_12, p_purpose_node_12], select=[p_promo_name_node_12, p_purpose_node_12, Partial_MAX(p_channel_catalog_node_12) AS max$0])
                                 +- Sort(orderBy=[p_promo_name_node_12 ASC, p_purpose_node_12 ASC])
                                    +- Calc(select=[p_promo_name_node_12, p_channel_catalog_node_12, p_purpose_node_12])
                                       +- Sort(orderBy=[p_item_sk_node_12 ASC])
                                          +- Exchange(distribution=[single])
                                             +- HashAggregate(isMerge=[false], groupBy=[3pPo4, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12], select=[3pPo4, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12])
                                                +- Calc(select=[p_promo_sk AS 3pPo4, p_promo_id AS p_promo_id_node_12, p_start_date_sk AS p_start_date_sk_node_12, p_end_date_sk AS p_end_date_sk_node_12, p_item_sk AS p_item_sk_node_12, p_cost AS p_cost_node_12, p_response_target AS p_response_target_node_12, p_promo_name AS p_promo_name_node_12, p_channel_dmail AS p_channel_dmail_node_12, p_channel_email AS p_channel_email_node_12, p_channel_catalog AS p_channel_catalog_node_12, p_channel_tv AS p_channel_tv_node_12, p_channel_radio AS p_channel_radio_node_12, p_channel_press AS p_channel_press_node_12, p_channel_event AS p_channel_event_node_12, p_channel_demo AS p_channel_demo_node_12, p_channel_details AS p_channel_details_node_12, p_purpose AS p_purpose_node_12, p_discount_active AS p_discount_active_node_12])
                                                   +- Exchange(distribution=[hash[p_item_sk]])
                                                      +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS p_channel_catalog_node_12])
+- SortAggregate(isMerge=[true], groupBy=[p_purpose_node_12], select=[p_purpose_node_12, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[p_purpose_node_12 ASC])
         +- Exchange(distribution=[hash[p_purpose_node_12]])
            +- LocalSortAggregate(groupBy=[p_purpose_node_12], select=[p_purpose_node_12, Partial_MAX(EXPR$0) AS max$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[p_purpose_node_12 ASC])
                     +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(p_channel_radio_node_11 = p_promo_name_node_12)], select=[p_channel_radio_node_11, p_promo_name_node_12, p_purpose_node_12, EXPR$0], isBroadcast=[true], build=[right])\
:- HashAggregate(isMerge=[true], groupBy=[p_channel_radio_node_11], select=[p_channel_radio_node_11])\
:  +- [#2] Exchange(distribution=[hash[p_channel_radio_node_11]])\
+- [#1] Exchange(distribution=[broadcast])\
])
                        :- Exchange(distribution=[broadcast])
                        :  +- SortAggregate(isMerge=[true], groupBy=[p_promo_name_node_12, p_purpose_node_12], select=[p_promo_name_node_12, p_purpose_node_12, Final_MAX(max$0) AS EXPR$0])
                        :     +- Exchange(distribution=[forward])
                        :        +- Sort(orderBy=[p_promo_name_node_12 ASC, p_purpose_node_12 ASC])
                        :           +- Exchange(distribution=[hash[p_promo_name_node_12, p_purpose_node_12]])
                        :              +- LocalSortAggregate(groupBy=[p_promo_name_node_12, p_purpose_node_12], select=[p_promo_name_node_12, p_purpose_node_12, Partial_MAX(p_channel_catalog_node_12) AS max$0])
                        :                 +- Exchange(distribution=[forward])
                        :                    +- Sort(orderBy=[p_promo_name_node_12 ASC, p_purpose_node_12 ASC])
                        :                       +- Calc(select=[p_promo_name_node_12, p_channel_catalog_node_12, p_purpose_node_12])
                        :                          +- Sort(orderBy=[p_item_sk_node_12 ASC])
                        :                             +- Exchange(distribution=[single])
                        :                                +- HashAggregate(isMerge=[false], groupBy=[3pPo4, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12], select=[3pPo4, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12])
                        :                                   +- Exchange(distribution=[keep_input_as_is[hash[3pPo4, p_promo_id_node_12, p_start_date_sk_node_12, p_end_date_sk_node_12, p_item_sk_node_12, p_cost_node_12, p_response_target_node_12, p_promo_name_node_12, p_channel_dmail_node_12, p_channel_email_node_12, p_channel_catalog_node_12, p_channel_tv_node_12, p_channel_radio_node_12, p_channel_press_node_12, p_channel_event_node_12, p_channel_demo_node_12, p_channel_details_node_12, p_purpose_node_12, p_discount_active_node_12]]])
                        :                                      +- Calc(select=[p_promo_sk AS 3pPo4, p_promo_id AS p_promo_id_node_12, p_start_date_sk AS p_start_date_sk_node_12, p_end_date_sk AS p_end_date_sk_node_12, p_item_sk AS p_item_sk_node_12, p_cost AS p_cost_node_12, p_response_target AS p_response_target_node_12, p_promo_name AS p_promo_name_node_12, p_channel_dmail AS p_channel_dmail_node_12, p_channel_email AS p_channel_email_node_12, p_channel_catalog AS p_channel_catalog_node_12, p_channel_tv AS p_channel_tv_node_12, p_channel_radio AS p_channel_radio_node_12, p_channel_press AS p_channel_press_node_12, p_channel_event AS p_channel_event_node_12, p_channel_demo AS p_channel_demo_node_12, p_channel_details AS p_channel_details_node_12, p_purpose AS p_purpose_node_12, p_discount_active AS p_discount_active_node_12])
                        :                                         +- Exchange(distribution=[hash[p_item_sk]])
                        :                                            +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])(reuse_id=[1])
                        +- Exchange(distribution=[hash[p_channel_radio_node_11]])
                           +- LocalHashAggregate(groupBy=[p_channel_radio_node_11], select=[p_channel_radio_node_11])
                              +- Calc(select=[EXPR$0 AS p_channel_radio_node_11])
                                 +- SortAggregate(isMerge=[false], groupBy=[cs_net_paid_inc_ship], select=[cs_net_paid_inc_ship, MIN(p_channel_radio_node_11) AS EXPR$0])
                                    +- Exchange(distribution=[forward])
                                       +- Sort(orderBy=[cs_net_paid_inc_ship ASC])
                                          +- Exchange(distribution=[hash[cs_net_paid_inc_ship]])
                                             +- HashJoin(joinType=[InnerJoin], where=[(p_item_sk_node_11 = cs_ship_customer_sk)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, NfRkv, p_promo_id_node_11, p_start_date_sk_node_11, p_end_date_sk_node_11, p_item_sk_node_11, p_cost_node_11, p_response_target_node_11, p_promo_name_node_11, p_channel_dmail_node_11, p_channel_email_node_11, p_channel_catalog_node_11, p_channel_tv_node_11, p_channel_radio_node_11, p_channel_press_node_11, p_channel_event_node_11, p_channel_demo_node_11, p_channel_details_node_11, p_purpose_node_11, p_discount_active_node_11], isBroadcast=[true], build=[right])
                                                :- Sort(orderBy=[cs_item_sk ASC])
                                                :  +- Exchange(distribution=[single])
                                                :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
                                                +- Exchange(distribution=[broadcast])
                                                   +- Calc(select=[p_promo_sk AS NfRkv, p_promo_id AS p_promo_id_node_11, p_start_date_sk AS p_start_date_sk_node_11, p_end_date_sk AS p_end_date_sk_node_11, p_item_sk AS p_item_sk_node_11, p_cost AS p_cost_node_11, p_response_target AS p_response_target_node_11, p_promo_name AS p_promo_name_node_11, p_channel_dmail AS p_channel_dmail_node_11, p_channel_email AS p_channel_email_node_11, p_channel_catalog AS p_channel_catalog_node_11, p_channel_tv AS p_channel_tv_node_11, p_channel_radio AS p_channel_radio_node_11, p_channel_press AS p_channel_press_node_11, p_channel_event AS p_channel_event_node_11, p_channel_demo AS p_channel_demo_node_11, p_channel_details AS p_channel_details_node_11, p_purpose AS p_purpose_node_11, p_discount_active AS p_discount_active_node_11])
                                                      +- Reused(reference_id=[1])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o42477465.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#84386567:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[15](input=RelSubset#84386565,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[15]), rel#84386564:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[15](input=RelSubset#84386563,groupBy=cs_ship_customer_sk, cs_net_paid_inc_ship,select=cs_ship_customer_sk, cs_net_paid_inc_ship)]
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