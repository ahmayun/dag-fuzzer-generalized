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
    return values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_25 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_25") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_24 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_24") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_21 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_21") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_20 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_20") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_23 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_23") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_22 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_22") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_19 = autonode_25.select(col('i_brand_id_node_25'))
autonode_18 = autonode_24.group_by(col('cp_start_date_sk_node_24')).select(col('cp_end_date_sk_node_24').avg.alias('cp_end_date_sk_node_24'))
autonode_15 = autonode_20.join(autonode_21, col('sr_hdemo_sk_node_20') == col('wp_access_date_sk_node_21'))
autonode_17 = autonode_23.order_by(col('wr_return_amt_inc_tax_node_23'))
autonode_16 = autonode_22.filter(col('ib_lower_bound_node_22') > -40)
autonode_14 = autonode_18.join(autonode_19, col('cp_end_date_sk_node_24') == col('i_brand_id_node_25'))
autonode_11 = autonode_15.distinct()
autonode_13 = autonode_17.filter(col('wr_returned_date_sk_node_23') >= 40)
autonode_12 = autonode_16.alias('4uqt9')
autonode_10 = autonode_14.distinct()
autonode_7 = autonode_11.add_columns(lit("hello"))
autonode_9 = autonode_13.add_columns(lit("hello"))
autonode_8 = autonode_12.distinct()
autonode_4 = autonode_7.filter(col('sr_reversed_charge_node_20') <= -5.824762582778931)
autonode_6 = autonode_9.join(autonode_10, col('cp_end_date_sk_node_24') == col('wr_web_page_sk_node_23'))
autonode_5 = autonode_8.add_columns(lit("hello"))
autonode_3 = autonode_6.group_by(col('wr_return_amt_inc_tax_node_23')).select(col('wr_return_tax_node_23').max.alias('wr_return_tax_node_23'))
autonode_2 = autonode_4.join(autonode_5, col('sr_customer_sk_node_20') == col('ib_lower_bound_node_22'))
autonode_1 = autonode_2.join(autonode_3, col('sr_return_tax_node_20') == col('wr_return_tax_node_23'))
sink = autonode_1.limit(14)
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
LogicalSort(fetch=[14])
+- LogicalJoin(condition=[=($12, $39)], joinType=[inner])
   :- LogicalJoin(condition=[=($3, $36)], joinType=[inner])
   :  :- LogicalFilter(condition=[<=($17, -5.824762582778931E0:DOUBLE)])
   :  :  +- LogicalProject(sr_returned_date_sk_node_20=[$0], sr_return_time_sk_node_20=[$1], sr_item_sk_node_20=[$2], sr_customer_sk_node_20=[$3], sr_cdemo_sk_node_20=[$4], sr_hdemo_sk_node_20=[$5], sr_addr_sk_node_20=[$6], sr_store_sk_node_20=[$7], sr_reason_sk_node_20=[$8], sr_ticket_number_node_20=[$9], sr_return_quantity_node_20=[$10], sr_return_amt_node_20=[$11], sr_return_tax_node_20=[$12], sr_return_amt_inc_tax_node_20=[$13], sr_fee_node_20=[$14], sr_return_ship_cost_node_20=[$15], sr_refunded_cash_node_20=[$16], sr_reversed_charge_node_20=[$17], sr_store_credit_node_20=[$18], sr_net_loss_node_20=[$19], wp_web_page_sk_node_21=[$20], wp_web_page_id_node_21=[$21], wp_rec_start_date_node_21=[$22], wp_rec_end_date_node_21=[$23], wp_creation_date_sk_node_21=[$24], wp_access_date_sk_node_21=[$25], wp_autogen_flag_node_21=[$26], wp_customer_sk_node_21=[$27], wp_url_node_21=[$28], wp_type_node_21=[$29], wp_char_count_node_21=[$30], wp_link_count_node_21=[$31], wp_image_count_node_21=[$32], wp_max_ad_count_node_21=[$33], _c34=[_UTF-16LE'hello'])
   :  :     +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33}])
   :  :        +- LogicalJoin(condition=[=($5, $25)], joinType=[inner])
   :  :           :- LogicalProject(sr_returned_date_sk_node_20=[$0], sr_return_time_sk_node_20=[$1], sr_item_sk_node_20=[$2], sr_customer_sk_node_20=[$3], sr_cdemo_sk_node_20=[$4], sr_hdemo_sk_node_20=[$5], sr_addr_sk_node_20=[$6], sr_store_sk_node_20=[$7], sr_reason_sk_node_20=[$8], sr_ticket_number_node_20=[$9], sr_return_quantity_node_20=[$10], sr_return_amt_node_20=[$11], sr_return_tax_node_20=[$12], sr_return_amt_inc_tax_node_20=[$13], sr_fee_node_20=[$14], sr_return_ship_cost_node_20=[$15], sr_refunded_cash_node_20=[$16], sr_reversed_charge_node_20=[$17], sr_store_credit_node_20=[$18], sr_net_loss_node_20=[$19])
   :  :           :  +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
   :  :           +- LogicalProject(wp_web_page_sk_node_21=[$0], wp_web_page_id_node_21=[$1], wp_rec_start_date_node_21=[$2], wp_rec_end_date_node_21=[$3], wp_creation_date_sk_node_21=[$4], wp_access_date_sk_node_21=[$5], wp_autogen_flag_node_21=[$6], wp_customer_sk_node_21=[$7], wp_url_node_21=[$8], wp_type_node_21=[$9], wp_char_count_node_21=[$10], wp_link_count_node_21=[$11], wp_image_count_node_21=[$12], wp_max_ad_count_node_21=[$13])
   :  :              +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
   :  +- LogicalProject(4uqt9=[$0], ib_lower_bound_node_22=[$1], ib_upper_bound_node_22=[$2], _c3=[_UTF-16LE'hello'])
   :     +- LogicalAggregate(group=[{0, 1, 2}])
   :        +- LogicalProject(4uqt9=[AS($0, _UTF-16LE'4uqt9')], ib_lower_bound_node_22=[$1], ib_upper_bound_node_22=[$2])
   :           +- LogicalFilter(condition=[>($1, -40)])
   :              +- LogicalProject(ib_income_band_sk_node_22=[$0], ib_lower_bound_node_22=[$1], ib_upper_bound_node_22=[$2])
   :                 +- LogicalTableScan(table=[[default_catalog, default_database, income_band]])
   +- LogicalProject(wr_return_tax_node_23=[$1])
      +- LogicalAggregate(group=[{17}], EXPR$0=[MAX($16)])
         +- LogicalJoin(condition=[=($25, $11)], joinType=[inner])
            :- LogicalProject(wr_returned_date_sk_node_23=[$0], wr_returned_time_sk_node_23=[$1], wr_item_sk_node_23=[$2], wr_refunded_customer_sk_node_23=[$3], wr_refunded_cdemo_sk_node_23=[$4], wr_refunded_hdemo_sk_node_23=[$5], wr_refunded_addr_sk_node_23=[$6], wr_returning_customer_sk_node_23=[$7], wr_returning_cdemo_sk_node_23=[$8], wr_returning_hdemo_sk_node_23=[$9], wr_returning_addr_sk_node_23=[$10], wr_web_page_sk_node_23=[$11], wr_reason_sk_node_23=[$12], wr_order_number_node_23=[$13], wr_return_quantity_node_23=[$14], wr_return_amt_node_23=[$15], wr_return_tax_node_23=[$16], wr_return_amt_inc_tax_node_23=[$17], wr_fee_node_23=[$18], wr_return_ship_cost_node_23=[$19], wr_refunded_cash_node_23=[$20], wr_reversed_charge_node_23=[$21], wr_account_credit_node_23=[$22], wr_net_loss_node_23=[$23], _c24=[_UTF-16LE'hello'])
            :  +- LogicalFilter(condition=[>=($0, 40)])
            :     +- LogicalSort(sort0=[$17], dir0=[ASC])
            :        +- LogicalProject(wr_returned_date_sk_node_23=[$0], wr_returned_time_sk_node_23=[$1], wr_item_sk_node_23=[$2], wr_refunded_customer_sk_node_23=[$3], wr_refunded_cdemo_sk_node_23=[$4], wr_refunded_hdemo_sk_node_23=[$5], wr_refunded_addr_sk_node_23=[$6], wr_returning_customer_sk_node_23=[$7], wr_returning_cdemo_sk_node_23=[$8], wr_returning_hdemo_sk_node_23=[$9], wr_returning_addr_sk_node_23=[$10], wr_web_page_sk_node_23=[$11], wr_reason_sk_node_23=[$12], wr_order_number_node_23=[$13], wr_return_quantity_node_23=[$14], wr_return_amt_node_23=[$15], wr_return_tax_node_23=[$16], wr_return_amt_inc_tax_node_23=[$17], wr_fee_node_23=[$18], wr_return_ship_cost_node_23=[$19], wr_refunded_cash_node_23=[$20], wr_reversed_charge_node_23=[$21], wr_account_credit_node_23=[$22], wr_net_loss_node_23=[$23])
            :           +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
            +- LogicalAggregate(group=[{0, 1}])
               +- LogicalJoin(condition=[=($0, $1)], joinType=[inner])
                  :- LogicalProject(cp_end_date_sk_node_24=[$1])
                  :  +- LogicalAggregate(group=[{0}], EXPR$0=[AVG($1)])
                  :     +- LogicalProject(cp_start_date_sk_node_24=[$2], cp_end_date_sk_node_24=[$3])
                  :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])
                  +- LogicalProject(i_brand_id_node_25=[$7])
                     +- LogicalTableScan(table=[[default_catalog, default_database, item]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[14], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[14], global=[false])
      +- HashJoin(joinType=[InnerJoin], where=[=(sr_return_tax_node_20, wr_return_tax_node_23)], select=[sr_returned_date_sk_node_20, sr_return_time_sk_node_20, sr_item_sk_node_20, sr_customer_sk_node_20, sr_cdemo_sk_node_20, sr_hdemo_sk_node_20, sr_addr_sk_node_20, sr_store_sk_node_20, sr_reason_sk_node_20, sr_ticket_number_node_20, sr_return_quantity_node_20, sr_return_amt_node_20, sr_return_tax_node_20, sr_return_amt_inc_tax_node_20, sr_fee_node_20, sr_return_ship_cost_node_20, sr_refunded_cash_node_20, sr_reversed_charge_node_20, sr_store_credit_node_20, sr_net_loss_node_20, wp_web_page_sk_node_21, wp_web_page_id_node_21, wp_rec_start_date_node_21, wp_rec_end_date_node_21, wp_creation_date_sk_node_21, wp_access_date_sk_node_21, wp_autogen_flag_node_21, wp_customer_sk_node_21, wp_url_node_21, wp_type_node_21, wp_char_count_node_21, wp_link_count_node_21, wp_image_count_node_21, wp_max_ad_count_node_21, _c34, 4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22, _c3, wr_return_tax_node_23], isBroadcast=[true], build=[right])
         :- NestedLoopJoin(joinType=[InnerJoin], where=[=(sr_customer_sk_node_20, ib_lower_bound_node_22)], select=[sr_returned_date_sk_node_20, sr_return_time_sk_node_20, sr_item_sk_node_20, sr_customer_sk_node_20, sr_cdemo_sk_node_20, sr_hdemo_sk_node_20, sr_addr_sk_node_20, sr_store_sk_node_20, sr_reason_sk_node_20, sr_ticket_number_node_20, sr_return_quantity_node_20, sr_return_amt_node_20, sr_return_tax_node_20, sr_return_amt_inc_tax_node_20, sr_fee_node_20, sr_return_ship_cost_node_20, sr_refunded_cash_node_20, sr_reversed_charge_node_20, sr_store_credit_node_20, sr_net_loss_node_20, wp_web_page_sk_node_21, wp_web_page_id_node_21, wp_rec_start_date_node_21, wp_rec_end_date_node_21, wp_creation_date_sk_node_21, wp_access_date_sk_node_21, wp_autogen_flag_node_21, wp_customer_sk_node_21, wp_url_node_21, wp_type_node_21, wp_char_count_node_21, wp_link_count_node_21, wp_image_count_node_21, wp_max_ad_count_node_21, _c34, 4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22, _c3], build=[right])
         :  :- Calc(select=[sr_returned_date_sk AS sr_returned_date_sk_node_20, sr_return_time_sk AS sr_return_time_sk_node_20, sr_item_sk AS sr_item_sk_node_20, sr_customer_sk AS sr_customer_sk_node_20, sr_cdemo_sk AS sr_cdemo_sk_node_20, sr_hdemo_sk AS sr_hdemo_sk_node_20, sr_addr_sk AS sr_addr_sk_node_20, sr_store_sk AS sr_store_sk_node_20, sr_reason_sk AS sr_reason_sk_node_20, sr_ticket_number AS sr_ticket_number_node_20, sr_return_quantity AS sr_return_quantity_node_20, sr_return_amt AS sr_return_amt_node_20, sr_return_tax AS sr_return_tax_node_20, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_20, sr_fee AS sr_fee_node_20, sr_return_ship_cost AS sr_return_ship_cost_node_20, sr_refunded_cash AS sr_refunded_cash_node_20, sr_reversed_charge AS sr_reversed_charge_node_20, sr_store_credit AS sr_store_credit_node_20, sr_net_loss AS sr_net_loss_node_20, wp_web_page_sk AS wp_web_page_sk_node_21, wp_web_page_id AS wp_web_page_id_node_21, wp_rec_start_date AS wp_rec_start_date_node_21, wp_rec_end_date AS wp_rec_end_date_node_21, wp_creation_date_sk AS wp_creation_date_sk_node_21, wp_access_date_sk AS wp_access_date_sk_node_21, wp_autogen_flag AS wp_autogen_flag_node_21, wp_customer_sk AS wp_customer_sk_node_21, wp_url AS wp_url_node_21, wp_type AS wp_type_node_21, wp_char_count AS wp_char_count_node_21, wp_link_count AS wp_link_count_node_21, wp_image_count AS wp_image_count_node_21, wp_max_ad_count AS wp_max_ad_count_node_21, 'hello' AS _c34])
         :  :  +- HashAggregate(isMerge=[false], groupBy=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
         :  :     +- Exchange(distribution=[hash[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count]])
         :  :        +- HashJoin(joinType=[InnerJoin], where=[=(sr_hdemo_sk, wp_access_date_sk)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], isBroadcast=[true], build=[right])
         :  :           :- Calc(select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], where=[<=(sr_reversed_charge, -5.824762582778931E0)])
         :  :           :  +- TableSourceScan(table=[[default_catalog, default_database, store_returns, filter=[<=(sr_reversed_charge, -5.824762582778931E0:DOUBLE)]]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
         :  :           +- Exchange(distribution=[broadcast])
         :  :              +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
         :  +- Exchange(distribution=[broadcast])
         :     +- Calc(select=[4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22, 'hello' AS _c3])
         :        +- HashAggregate(isMerge=[true], groupBy=[4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22], select=[4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22])
         :           +- Exchange(distribution=[hash[4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22]])
         :              +- LocalHashAggregate(groupBy=[4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22], select=[4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22])
         :                 +- Calc(select=[ib_income_band_sk AS 4uqt9, ib_lower_bound AS ib_lower_bound_node_22, ib_upper_bound AS ib_upper_bound_node_22], where=[>(ib_lower_bound, -40)])
         :                    +- TableSourceScan(table=[[default_catalog, default_database, income_band, filter=[>(ib_lower_bound, -40)]]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[EXPR$0 AS wr_return_tax_node_23])
               +- HashAggregate(isMerge=[true], groupBy=[wr_return_amt_inc_tax_node_23], select=[wr_return_amt_inc_tax_node_23, Final_MAX(max$0) AS EXPR$0])
                  +- Exchange(distribution=[hash[wr_return_amt_inc_tax_node_23]])
                     +- LocalHashAggregate(groupBy=[wr_return_amt_inc_tax_node_23], select=[wr_return_amt_inc_tax_node_23, Partial_MAX(wr_return_tax_node_23) AS max$0])
                        +- HashJoin(joinType=[InnerJoin], where=[=(cp_end_date_sk_node_24, wr_web_page_sk_node_23)], select=[wr_returned_date_sk_node_23, wr_returned_time_sk_node_23, wr_item_sk_node_23, wr_refunded_customer_sk_node_23, wr_refunded_cdemo_sk_node_23, wr_refunded_hdemo_sk_node_23, wr_refunded_addr_sk_node_23, wr_returning_customer_sk_node_23, wr_returning_cdemo_sk_node_23, wr_returning_hdemo_sk_node_23, wr_returning_addr_sk_node_23, wr_web_page_sk_node_23, wr_reason_sk_node_23, wr_order_number_node_23, wr_return_quantity_node_23, wr_return_amt_node_23, wr_return_tax_node_23, wr_return_amt_inc_tax_node_23, wr_fee_node_23, wr_return_ship_cost_node_23, wr_refunded_cash_node_23, wr_reversed_charge_node_23, wr_account_credit_node_23, wr_net_loss_node_23, _c24, cp_end_date_sk_node_24, i_brand_id], isBroadcast=[true], build=[right])
                           :- Calc(select=[wr_returned_date_sk AS wr_returned_date_sk_node_23, wr_returned_time_sk AS wr_returned_time_sk_node_23, wr_item_sk AS wr_item_sk_node_23, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_23, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_23, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_23, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_23, wr_returning_customer_sk AS wr_returning_customer_sk_node_23, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_23, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_23, wr_returning_addr_sk AS wr_returning_addr_sk_node_23, wr_web_page_sk AS wr_web_page_sk_node_23, wr_reason_sk AS wr_reason_sk_node_23, wr_order_number AS wr_order_number_node_23, wr_return_quantity AS wr_return_quantity_node_23, wr_return_amt AS wr_return_amt_node_23, wr_return_tax AS wr_return_tax_node_23, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_23, wr_fee AS wr_fee_node_23, wr_return_ship_cost AS wr_return_ship_cost_node_23, wr_refunded_cash AS wr_refunded_cash_node_23, wr_reversed_charge AS wr_reversed_charge_node_23, wr_account_credit AS wr_account_credit_node_23, wr_net_loss AS wr_net_loss_node_23, 'hello' AS _c24], where=[>=(wr_returned_date_sk, 40)])
                           :  +- Sort(orderBy=[wr_return_amt_inc_tax ASC])
                           :     +- Exchange(distribution=[single])
                           :        +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
                           +- Exchange(distribution=[broadcast])
                              +- HashJoin(joinType=[InnerJoin], where=[=(cp_end_date_sk_node_24, i_brand_id)], select=[cp_end_date_sk_node_24, i_brand_id], isBroadcast=[true], build=[left])
                                 :- Exchange(distribution=[broadcast])
                                 :  +- HashAggregate(isMerge=[true], groupBy=[cp_end_date_sk_node_24], select=[cp_end_date_sk_node_24])
                                 :     +- Exchange(distribution=[hash[cp_end_date_sk_node_24]])
                                 :        +- LocalHashAggregate(groupBy=[cp_end_date_sk_node_24], select=[cp_end_date_sk_node_24])
                                 :           +- Calc(select=[EXPR$0 AS cp_end_date_sk_node_24])
                                 :              +- HashAggregate(isMerge=[true], groupBy=[cp_start_date_sk], select=[cp_start_date_sk, Final_AVG(sum$0, count$1) AS EXPR$0])
                                 :                 +- Exchange(distribution=[hash[cp_start_date_sk]])
                                 :                    +- LocalHashAggregate(groupBy=[cp_start_date_sk], select=[cp_start_date_sk, Partial_AVG(cp_end_date_sk) AS (sum$0, count$1)])
                                 :                       +- TableSourceScan(table=[[default_catalog, default_database, catalog_page, project=[cp_start_date_sk, cp_end_date_sk], metadata=[]]], fields=[cp_start_date_sk, cp_end_date_sk])
                                 +- HashAggregate(isMerge=[true], groupBy=[i_brand_id], select=[i_brand_id])
                                    +- Exchange(distribution=[hash[i_brand_id]])
                                       +- LocalHashAggregate(groupBy=[i_brand_id], select=[i_brand_id])
                                          +- TableSourceScan(table=[[default_catalog, default_database, item, project=[i_brand_id], metadata=[]]], fields=[i_brand_id])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[14], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[14], global=[false])
      +- MultipleInput(readOrder=[0,0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(sr_return_tax_node_20 = wr_return_tax_node_23)], select=[sr_returned_date_sk_node_20, sr_return_time_sk_node_20, sr_item_sk_node_20, sr_customer_sk_node_20, sr_cdemo_sk_node_20, sr_hdemo_sk_node_20, sr_addr_sk_node_20, sr_store_sk_node_20, sr_reason_sk_node_20, sr_ticket_number_node_20, sr_return_quantity_node_20, sr_return_amt_node_20, sr_return_tax_node_20, sr_return_amt_inc_tax_node_20, sr_fee_node_20, sr_return_ship_cost_node_20, sr_refunded_cash_node_20, sr_reversed_charge_node_20, sr_store_credit_node_20, sr_net_loss_node_20, wp_web_page_sk_node_21, wp_web_page_id_node_21, wp_rec_start_date_node_21, wp_rec_end_date_node_21, wp_creation_date_sk_node_21, wp_access_date_sk_node_21, wp_autogen_flag_node_21, wp_customer_sk_node_21, wp_url_node_21, wp_type_node_21, wp_char_count_node_21, wp_link_count_node_21, wp_image_count_node_21, wp_max_ad_count_node_21, _c34, 4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22, _c3, wr_return_tax_node_23], isBroadcast=[true], build=[right])\
:- NestedLoopJoin(joinType=[InnerJoin], where=[(sr_customer_sk_node_20 = ib_lower_bound_node_22)], select=[sr_returned_date_sk_node_20, sr_return_time_sk_node_20, sr_item_sk_node_20, sr_customer_sk_node_20, sr_cdemo_sk_node_20, sr_hdemo_sk_node_20, sr_addr_sk_node_20, sr_store_sk_node_20, sr_reason_sk_node_20, sr_ticket_number_node_20, sr_return_quantity_node_20, sr_return_amt_node_20, sr_return_tax_node_20, sr_return_amt_inc_tax_node_20, sr_fee_node_20, sr_return_ship_cost_node_20, sr_refunded_cash_node_20, sr_reversed_charge_node_20, sr_store_credit_node_20, sr_net_loss_node_20, wp_web_page_sk_node_21, wp_web_page_id_node_21, wp_rec_start_date_node_21, wp_rec_end_date_node_21, wp_creation_date_sk_node_21, wp_access_date_sk_node_21, wp_autogen_flag_node_21, wp_customer_sk_node_21, wp_url_node_21, wp_type_node_21, wp_char_count_node_21, wp_link_count_node_21, wp_image_count_node_21, wp_max_ad_count_node_21, _c34, 4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22, _c3], build=[right])\
:  :- Calc(select=[sr_returned_date_sk AS sr_returned_date_sk_node_20, sr_return_time_sk AS sr_return_time_sk_node_20, sr_item_sk AS sr_item_sk_node_20, sr_customer_sk AS sr_customer_sk_node_20, sr_cdemo_sk AS sr_cdemo_sk_node_20, sr_hdemo_sk AS sr_hdemo_sk_node_20, sr_addr_sk AS sr_addr_sk_node_20, sr_store_sk AS sr_store_sk_node_20, sr_reason_sk AS sr_reason_sk_node_20, sr_ticket_number AS sr_ticket_number_node_20, sr_return_quantity AS sr_return_quantity_node_20, sr_return_amt AS sr_return_amt_node_20, sr_return_tax AS sr_return_tax_node_20, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_20, sr_fee AS sr_fee_node_20, sr_return_ship_cost AS sr_return_ship_cost_node_20, sr_refunded_cash AS sr_refunded_cash_node_20, sr_reversed_charge AS sr_reversed_charge_node_20, sr_store_credit AS sr_store_credit_node_20, sr_net_loss AS sr_net_loss_node_20, wp_web_page_sk AS wp_web_page_sk_node_21, wp_web_page_id AS wp_web_page_id_node_21, wp_rec_start_date AS wp_rec_start_date_node_21, wp_rec_end_date AS wp_rec_end_date_node_21, wp_creation_date_sk AS wp_creation_date_sk_node_21, wp_access_date_sk AS wp_access_date_sk_node_21, wp_autogen_flag AS wp_autogen_flag_node_21, wp_customer_sk AS wp_customer_sk_node_21, wp_url AS wp_url_node_21, wp_type AS wp_type_node_21, wp_char_count AS wp_char_count_node_21, wp_link_count AS wp_link_count_node_21, wp_image_count AS wp_image_count_node_21, wp_max_ad_count AS wp_max_ad_count_node_21, 'hello' AS _c34])\
:  :  +- HashAggregate(isMerge=[false], groupBy=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])\
:  :     +- [#3] Exchange(distribution=[hash[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count]])\
:  +- [#2] Exchange(distribution=[broadcast])\
+- [#1] Exchange(distribution=[broadcast])\
])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[EXPR$0 AS wr_return_tax_node_23])
         :     +- HashAggregate(isMerge=[true], groupBy=[wr_return_amt_inc_tax_node_23], select=[wr_return_amt_inc_tax_node_23, Final_MAX(max$0) AS EXPR$0])
         :        +- Exchange(distribution=[hash[wr_return_amt_inc_tax_node_23]])
         :           +- LocalHashAggregate(groupBy=[wr_return_amt_inc_tax_node_23], select=[wr_return_amt_inc_tax_node_23, Partial_MAX(wr_return_tax_node_23) AS max$0])
         :              +- HashJoin(joinType=[InnerJoin], where=[(cp_end_date_sk_node_24 = wr_web_page_sk_node_23)], select=[wr_returned_date_sk_node_23, wr_returned_time_sk_node_23, wr_item_sk_node_23, wr_refunded_customer_sk_node_23, wr_refunded_cdemo_sk_node_23, wr_refunded_hdemo_sk_node_23, wr_refunded_addr_sk_node_23, wr_returning_customer_sk_node_23, wr_returning_cdemo_sk_node_23, wr_returning_hdemo_sk_node_23, wr_returning_addr_sk_node_23, wr_web_page_sk_node_23, wr_reason_sk_node_23, wr_order_number_node_23, wr_return_quantity_node_23, wr_return_amt_node_23, wr_return_tax_node_23, wr_return_amt_inc_tax_node_23, wr_fee_node_23, wr_return_ship_cost_node_23, wr_refunded_cash_node_23, wr_reversed_charge_node_23, wr_account_credit_node_23, wr_net_loss_node_23, _c24, cp_end_date_sk_node_24, i_brand_id], isBroadcast=[true], build=[right])
         :                 :- Calc(select=[wr_returned_date_sk AS wr_returned_date_sk_node_23, wr_returned_time_sk AS wr_returned_time_sk_node_23, wr_item_sk AS wr_item_sk_node_23, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_23, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_23, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_23, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_23, wr_returning_customer_sk AS wr_returning_customer_sk_node_23, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_23, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_23, wr_returning_addr_sk AS wr_returning_addr_sk_node_23, wr_web_page_sk AS wr_web_page_sk_node_23, wr_reason_sk AS wr_reason_sk_node_23, wr_order_number AS wr_order_number_node_23, wr_return_quantity AS wr_return_quantity_node_23, wr_return_amt AS wr_return_amt_node_23, wr_return_tax AS wr_return_tax_node_23, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_23, wr_fee AS wr_fee_node_23, wr_return_ship_cost AS wr_return_ship_cost_node_23, wr_refunded_cash AS wr_refunded_cash_node_23, wr_reversed_charge AS wr_reversed_charge_node_23, wr_account_credit AS wr_account_credit_node_23, wr_net_loss AS wr_net_loss_node_23, 'hello' AS _c24], where=[(wr_returned_date_sk >= 40)])
         :                 :  +- Sort(orderBy=[wr_return_amt_inc_tax ASC])
         :                 :     +- Exchange(distribution=[single])
         :                 :        +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
         :                 +- Exchange(distribution=[broadcast])
         :                    +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(cp_end_date_sk_node_24 = i_brand_id)], select=[cp_end_date_sk_node_24, i_brand_id], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- HashAggregate(isMerge=[true], groupBy=[i_brand_id], select=[i_brand_id])\
   +- [#2] Exchange(distribution=[hash[i_brand_id]])\
])
         :                       :- Exchange(distribution=[broadcast])
         :                       :  +- HashAggregate(isMerge=[true], groupBy=[cp_end_date_sk_node_24], select=[cp_end_date_sk_node_24])
         :                       :     +- Exchange(distribution=[hash[cp_end_date_sk_node_24]])
         :                       :        +- LocalHashAggregate(groupBy=[cp_end_date_sk_node_24], select=[cp_end_date_sk_node_24])
         :                       :           +- Calc(select=[EXPR$0 AS cp_end_date_sk_node_24])
         :                       :              +- HashAggregate(isMerge=[true], groupBy=[cp_start_date_sk], select=[cp_start_date_sk, Final_AVG(sum$0, count$1) AS EXPR$0])
         :                       :                 +- Exchange(distribution=[hash[cp_start_date_sk]])
         :                       :                    +- LocalHashAggregate(groupBy=[cp_start_date_sk], select=[cp_start_date_sk, Partial_AVG(cp_end_date_sk) AS (sum$0, count$1)])
         :                       :                       +- TableSourceScan(table=[[default_catalog, default_database, catalog_page, project=[cp_start_date_sk, cp_end_date_sk], metadata=[]]], fields=[cp_start_date_sk, cp_end_date_sk])
         :                       +- Exchange(distribution=[hash[i_brand_id]])
         :                          +- LocalHashAggregate(groupBy=[i_brand_id], select=[i_brand_id])
         :                             +- TableSourceScan(table=[[default_catalog, default_database, item, project=[i_brand_id], metadata=[]]], fields=[i_brand_id])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22, 'hello' AS _c3])
         :     +- HashAggregate(isMerge=[true], groupBy=[4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22], select=[4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22])
         :        +- Exchange(distribution=[hash[4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22]])
         :           +- LocalHashAggregate(groupBy=[4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22], select=[4uqt9, ib_lower_bound_node_22, ib_upper_bound_node_22])
         :              +- Calc(select=[ib_income_band_sk AS 4uqt9, ib_lower_bound AS ib_lower_bound_node_22, ib_upper_bound AS ib_upper_bound_node_22], where=[(ib_lower_bound > -40)])
         :                 +- TableSourceScan(table=[[default_catalog, default_database, income_band, filter=[>(ib_lower_bound, -40)]]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
         +- Exchange(distribution=[hash[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count]])
            +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(sr_hdemo_sk = wp_access_date_sk)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], isBroadcast=[true], build=[right])\
:- Calc(select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], where=[(sr_reversed_charge <= -5.824762582778931E0)])\
:  +- [#2] TableSourceScan(table=[[default_catalog, default_database, store_returns, filter=[<=(sr_reversed_charge, -5.824762582778931E0:DOUBLE)]]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])\
+- [#1] Exchange(distribution=[broadcast])\
])
               :- Exchange(distribution=[broadcast])
               :  +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
               +- TableSourceScan(table=[[default_catalog, default_database, store_returns, filter=[<=(sr_reversed_charge, -5.824762582778931E0:DOUBLE)]]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o166842872.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#337354918:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[17](input=RelSubset#337354916,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[17]), rel#337354915:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[17](input=RelSubset#337354914,groupBy=wr_web_page_sk, wr_return_amt_inc_tax,select=wr_web_page_sk, wr_return_amt_inc_tax, Partial_MAX(wr_return_tax) AS max$0)]
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