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

autonode_24 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_24") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_18 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_18") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_19 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_19") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_21 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_21") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_20 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_20") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_23 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_23") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_22 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_22") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_17 = autonode_24.alias('5Rdqb')
autonode_13 = autonode_18.join(autonode_19, col('w_gmt_offset_node_18') == col('cs_ext_list_price_node_19'))
autonode_14 = autonode_20.join(autonode_21, col('d_dow_node_20') == col('sr_hdemo_sk_node_21'))
autonode_16 = autonode_23.filter(col('t_time_id_node_23').char_length >= 5)
autonode_15 = autonode_22.group_by(col('sr_fee_node_22')).select(call('preloaded_udf_agg', col('sr_item_sk_node_22')).alias('sr_item_sk_node_22'))
autonode_12 = autonode_17.add_columns(lit("hello"))
autonode_9 = autonode_13.join(autonode_14, col('w_suite_number_node_18') == col('d_quarter_name_node_20'))
autonode_11 = autonode_16.filter(col('t_time_id_node_23').char_length >= 5)
autonode_10 = autonode_15.distinct()
autonode_8 = autonode_12.distinct()
autonode_6 = autonode_9.order_by(col('sr_store_credit_node_21'))
autonode_7 = autonode_10.join(autonode_11, col('sr_item_sk_node_22') == col('t_hour_node_23'))
autonode_5 = autonode_8.alias('qZVlS')
autonode_4 = autonode_6.join(autonode_7, col('t_meal_time_node_23') == col('d_current_day_node_20'))
autonode_3 = autonode_5.filter(col('inv_item_sk_node_24') <= 16)
autonode_2 = autonode_4.group_by(col('d_holiday_node_20')).select(col('d_qoy_node_20').min.alias('d_qoy_node_20'))
autonode_1 = autonode_2.join(autonode_3, col('d_qoy_node_20') == col('inv_item_sk_node_24'))
sink = autonode_1.group_by(col('inv_warehouse_sk_node_24')).select(col('inv_quantity_on_hand_node_24').count.alias('inv_quantity_on_hand_node_24'))
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
LogicalProject(inv_quantity_on_hand_node_24=[$1])
+- LogicalAggregate(group=[{3}], EXPR$0=[COUNT($4)])
   +- LogicalJoin(condition=[=($0, $2)], joinType=[inner])
      :- LogicalProject(d_qoy_node_20=[$1])
      :  +- LogicalAggregate(group=[{64}], EXPR$0=[MIN($58)])
      :     +- LogicalJoin(condition=[=($106, $71)], joinType=[inner])
      :        :- LogicalSort(sort0=[$94], dir0=[ASC])
      :        :  +- LogicalJoin(condition=[=($7, $63)], joinType=[inner])
      :        :     :- LogicalJoin(condition=[=($13, $39)], joinType=[inner])
      :        :     :  :- LogicalProject(w_warehouse_sk_node_18=[$0], w_warehouse_id_node_18=[$1], w_warehouse_name_node_18=[$2], w_warehouse_sq_ft_node_18=[$3], w_street_number_node_18=[$4], w_street_name_node_18=[$5], w_street_type_node_18=[$6], w_suite_number_node_18=[$7], w_city_node_18=[$8], w_county_node_18=[$9], w_state_node_18=[$10], w_zip_node_18=[$11], w_country_node_18=[$12], w_gmt_offset_node_18=[$13])
      :        :     :  :  +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])
      :        :     :  +- LogicalProject(cs_sold_date_sk_node_19=[$0], cs_sold_time_sk_node_19=[$1], cs_ship_date_sk_node_19=[$2], cs_bill_customer_sk_node_19=[$3], cs_bill_cdemo_sk_node_19=[$4], cs_bill_hdemo_sk_node_19=[$5], cs_bill_addr_sk_node_19=[$6], cs_ship_customer_sk_node_19=[$7], cs_ship_cdemo_sk_node_19=[$8], cs_ship_hdemo_sk_node_19=[$9], cs_ship_addr_sk_node_19=[$10], cs_call_center_sk_node_19=[$11], cs_catalog_page_sk_node_19=[$12], cs_ship_mode_sk_node_19=[$13], cs_warehouse_sk_node_19=[$14], cs_item_sk_node_19=[$15], cs_promo_sk_node_19=[$16], cs_order_number_node_19=[$17], cs_quantity_node_19=[$18], cs_wholesale_cost_node_19=[$19], cs_list_price_node_19=[$20], cs_sales_price_node_19=[$21], cs_ext_discount_amt_node_19=[$22], cs_ext_sales_price_node_19=[$23], cs_ext_wholesale_cost_node_19=[$24], cs_ext_list_price_node_19=[$25], cs_ext_tax_node_19=[$26], cs_coupon_amt_node_19=[$27], cs_ext_ship_cost_node_19=[$28], cs_net_paid_node_19=[$29], cs_net_paid_inc_tax_node_19=[$30], cs_net_paid_inc_ship_node_19=[$31], cs_net_paid_inc_ship_tax_node_19=[$32], cs_net_profit_node_19=[$33])
      :        :     :     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
      :        :     +- LogicalJoin(condition=[=($7, $33)], joinType=[inner])
      :        :        :- LogicalProject(d_date_sk_node_20=[$0], d_date_id_node_20=[$1], d_date_node_20=[$2], d_month_seq_node_20=[$3], d_week_seq_node_20=[$4], d_quarter_seq_node_20=[$5], d_year_node_20=[$6], d_dow_node_20=[$7], d_moy_node_20=[$8], d_dom_node_20=[$9], d_qoy_node_20=[$10], d_fy_year_node_20=[$11], d_fy_quarter_seq_node_20=[$12], d_fy_week_seq_node_20=[$13], d_day_name_node_20=[$14], d_quarter_name_node_20=[$15], d_holiday_node_20=[$16], d_weekend_node_20=[$17], d_following_holiday_node_20=[$18], d_first_dom_node_20=[$19], d_last_dom_node_20=[$20], d_same_day_ly_node_20=[$21], d_same_day_lq_node_20=[$22], d_current_day_node_20=[$23], d_current_week_node_20=[$24], d_current_month_node_20=[$25], d_current_quarter_node_20=[$26], d_current_year_node_20=[$27])
      :        :        :  +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
      :        :        +- LogicalProject(sr_returned_date_sk_node_21=[$0], sr_return_time_sk_node_21=[$1], sr_item_sk_node_21=[$2], sr_customer_sk_node_21=[$3], sr_cdemo_sk_node_21=[$4], sr_hdemo_sk_node_21=[$5], sr_addr_sk_node_21=[$6], sr_store_sk_node_21=[$7], sr_reason_sk_node_21=[$8], sr_ticket_number_node_21=[$9], sr_return_quantity_node_21=[$10], sr_return_amt_node_21=[$11], sr_return_tax_node_21=[$12], sr_return_amt_inc_tax_node_21=[$13], sr_fee_node_21=[$14], sr_return_ship_cost_node_21=[$15], sr_refunded_cash_node_21=[$16], sr_reversed_charge_node_21=[$17], sr_store_credit_node_21=[$18], sr_net_loss_node_21=[$19])
      :        :           +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
      :        +- LogicalJoin(condition=[=($0, $4)], joinType=[inner])
      :           :- LogicalAggregate(group=[{0}])
      :           :  +- LogicalProject(sr_item_sk_node_22=[$1])
      :           :     +- LogicalAggregate(group=[{1}], EXPR$0=[preloaded_udf_agg($0)])
      :           :        +- LogicalProject(sr_item_sk_node_22=[$2], sr_fee_node_22=[$14])
      :           :           +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
      :           +- LogicalFilter(condition=[>=(CHAR_LENGTH($1), 5)])
      :              +- LogicalFilter(condition=[>=(CHAR_LENGTH($1), 5)])
      :                 +- LogicalProject(t_time_sk_node_23=[$0], t_time_id_node_23=[$1], t_time_node_23=[$2], t_hour_node_23=[$3], t_minute_node_23=[$4], t_second_node_23=[$5], t_am_pm_node_23=[$6], t_shift_node_23=[$7], t_sub_shift_node_23=[$8], t_meal_time_node_23=[$9])
      :                    +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
      +- LogicalFilter(condition=[<=($1, 16)])
         +- LogicalProject(qZVlS=[AS($0, _UTF-16LE'qZVlS')], inv_item_sk_node_24=[$1], inv_warehouse_sk_node_24=[$2], inv_quantity_on_hand_node_24=[$3], _c4=[$4])
            +- LogicalAggregate(group=[{0, 1, 2, 3, 4}])
               +- LogicalProject(5Rdqb=[AS($0, _UTF-16LE'5Rdqb')], inv_item_sk_node_24=[$1], inv_warehouse_sk_node_24=[$2], inv_quantity_on_hand_node_24=[$3], _c4=[_UTF-16LE'hello'])
                  +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS inv_quantity_on_hand_node_24])
+- HashAggregate(isMerge=[true], groupBy=[inv_warehouse_sk_node_24], select=[inv_warehouse_sk_node_24, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[hash[inv_warehouse_sk_node_24]])
      +- LocalHashAggregate(groupBy=[inv_warehouse_sk_node_24], select=[inv_warehouse_sk_node_24, Partial_COUNT(inv_quantity_on_hand_node_24) AS count$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(d_qoy_node_20, inv_item_sk_node_24)], select=[d_qoy_node_20, qZVlS, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24, _c4], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[EXPR$0 AS d_qoy_node_20])
            :     +- HashAggregate(isMerge=[false], groupBy=[d_holiday], select=[d_holiday, MIN(d_qoy) AS EXPR$0])
            :        +- Exchange(distribution=[hash[d_holiday]])
            :           +- NestedLoopJoin(joinType=[InnerJoin], where=[=(t_meal_time_node_23, d_current_day)], select=[w_warehouse_sk_node_18, w_warehouse_id_node_18, w_warehouse_name_node_18, w_warehouse_sq_ft_node_18, w_street_number_node_18, w_street_name_node_18, w_street_type_node_18, w_suite_number_node_18, w_city_node_18, w_county_node_18, w_state_node_18, w_zip_node_18, w_country_node_18, w_gmt_offset_node_18, cs_sold_date_sk_node_19, cs_sold_time_sk_node_19, cs_ship_date_sk_node_19, cs_bill_customer_sk_node_19, cs_bill_cdemo_sk_node_19, cs_bill_hdemo_sk_node_19, cs_bill_addr_sk_node_19, cs_ship_customer_sk_node_19, cs_ship_cdemo_sk_node_19, cs_ship_hdemo_sk_node_19, cs_ship_addr_sk_node_19, cs_call_center_sk_node_19, cs_catalog_page_sk_node_19, cs_ship_mode_sk_node_19, cs_warehouse_sk_node_19, cs_item_sk_node_19, cs_promo_sk_node_19, cs_order_number_node_19, cs_quantity_node_19, cs_wholesale_cost_node_19, cs_list_price_node_19, cs_sales_price_node_19, cs_ext_discount_amt_node_19, cs_ext_sales_price_node_19, cs_ext_wholesale_cost_node_19, cs_ext_list_price_node_19, cs_ext_tax_node_19, cs_coupon_amt_node_19, cs_ext_ship_cost_node_19, cs_net_paid_node_19, cs_net_paid_inc_tax_node_19, cs_net_paid_inc_ship_node_19, cs_net_paid_inc_ship_tax_node_19, cs_net_profit_node_19, d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, sr_item_sk_node_22, t_time_sk_node_23, t_time_id_node_23, t_time_node_23, t_hour_node_23, t_minute_node_23, t_second_node_23, t_am_pm_node_23, t_shift_node_23, t_sub_shift_node_23, t_meal_time_node_23], build=[right])
            :              :- Sort(orderBy=[sr_store_credit ASC])
            :              :  +- Exchange(distribution=[single])
            :              :     +- HashJoin(joinType=[InnerJoin], where=[=(w_suite_number_node_18, d_quarter_name)], select=[w_warehouse_sk_node_18, w_warehouse_id_node_18, w_warehouse_name_node_18, w_warehouse_sq_ft_node_18, w_street_number_node_18, w_street_name_node_18, w_street_type_node_18, w_suite_number_node_18, w_city_node_18, w_county_node_18, w_state_node_18, w_zip_node_18, w_country_node_18, w_gmt_offset_node_18, cs_sold_date_sk_node_19, cs_sold_time_sk_node_19, cs_ship_date_sk_node_19, cs_bill_customer_sk_node_19, cs_bill_cdemo_sk_node_19, cs_bill_hdemo_sk_node_19, cs_bill_addr_sk_node_19, cs_ship_customer_sk_node_19, cs_ship_cdemo_sk_node_19, cs_ship_hdemo_sk_node_19, cs_ship_addr_sk_node_19, cs_call_center_sk_node_19, cs_catalog_page_sk_node_19, cs_ship_mode_sk_node_19, cs_warehouse_sk_node_19, cs_item_sk_node_19, cs_promo_sk_node_19, cs_order_number_node_19, cs_quantity_node_19, cs_wholesale_cost_node_19, cs_list_price_node_19, cs_sales_price_node_19, cs_ext_discount_amt_node_19, cs_ext_sales_price_node_19, cs_ext_wholesale_cost_node_19, cs_ext_list_price_node_19, cs_ext_tax_node_19, cs_coupon_amt_node_19, cs_ext_ship_cost_node_19, cs_net_paid_node_19, cs_net_paid_inc_tax_node_19, cs_net_paid_inc_ship_node_19, cs_net_paid_inc_ship_tax_node_19, cs_net_profit_node_19, d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], build=[right])
            :              :        :- Exchange(distribution=[hash[w_suite_number_node_18]])
            :              :        :  +- Calc(select=[w_warehouse_sk_node_18, w_warehouse_id_node_18, w_warehouse_name_node_18, w_warehouse_sq_ft_node_18, w_street_number_node_18, w_street_name_node_18, w_street_type_node_18, w_suite_number_node_18, w_city_node_18, w_county_node_18, w_state_node_18, w_zip_node_18, w_country_node_18, w_gmt_offset_node_18, cs_sold_date_sk AS cs_sold_date_sk_node_19, cs_sold_time_sk AS cs_sold_time_sk_node_19, cs_ship_date_sk AS cs_ship_date_sk_node_19, cs_bill_customer_sk AS cs_bill_customer_sk_node_19, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_19, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_19, cs_bill_addr_sk AS cs_bill_addr_sk_node_19, cs_ship_customer_sk AS cs_ship_customer_sk_node_19, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_19, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_19, cs_ship_addr_sk AS cs_ship_addr_sk_node_19, cs_call_center_sk AS cs_call_center_sk_node_19, cs_catalog_page_sk AS cs_catalog_page_sk_node_19, cs_ship_mode_sk AS cs_ship_mode_sk_node_19, cs_warehouse_sk AS cs_warehouse_sk_node_19, cs_item_sk AS cs_item_sk_node_19, cs_promo_sk AS cs_promo_sk_node_19, cs_order_number AS cs_order_number_node_19, cs_quantity AS cs_quantity_node_19, cs_wholesale_cost AS cs_wholesale_cost_node_19, cs_list_price AS cs_list_price_node_19, cs_sales_price AS cs_sales_price_node_19, cs_ext_discount_amt AS cs_ext_discount_amt_node_19, cs_ext_sales_price AS cs_ext_sales_price_node_19, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_19, cs_ext_list_price AS cs_ext_list_price_node_19, cs_ext_tax AS cs_ext_tax_node_19, cs_coupon_amt AS cs_coupon_amt_node_19, cs_ext_ship_cost AS cs_ext_ship_cost_node_19, cs_net_paid AS cs_net_paid_node_19, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_19, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_19, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_19, cs_net_profit AS cs_net_profit_node_19])
            :              :        :     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(w_gmt_offset_node_180, cs_ext_list_price)], select=[w_warehouse_sk_node_18, w_warehouse_id_node_18, w_warehouse_name_node_18, w_warehouse_sq_ft_node_18, w_street_number_node_18, w_street_name_node_18, w_street_type_node_18, w_suite_number_node_18, w_city_node_18, w_county_node_18, w_state_node_18, w_zip_node_18, w_country_node_18, w_gmt_offset_node_18, w_gmt_offset_node_180, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], build=[left])
            :              :        :        :- Exchange(distribution=[broadcast])
            :              :        :        :  +- Calc(select=[w_warehouse_sk AS w_warehouse_sk_node_18, w_warehouse_id AS w_warehouse_id_node_18, w_warehouse_name AS w_warehouse_name_node_18, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_18, w_street_number AS w_street_number_node_18, w_street_name AS w_street_name_node_18, w_street_type AS w_street_type_node_18, w_suite_number AS w_suite_number_node_18, w_city AS w_city_node_18, w_county AS w_county_node_18, w_state AS w_state_node_18, w_zip AS w_zip_node_18, w_country AS w_country_node_18, w_gmt_offset AS w_gmt_offset_node_18, CAST(w_gmt_offset AS DECIMAL(7, 2)) AS w_gmt_offset_node_180])
            :              :        :        :     +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
            :              :        :        +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
            :              :        +- Exchange(distribution=[hash[d_quarter_name]])
            :              :           +- HashJoin(joinType=[InnerJoin], where=[=(d_dow, sr_hdemo_sk)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], build=[left])
            :              :              :- Exchange(distribution=[hash[d_dow]])
            :              :              :  +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
            :              :              +- Exchange(distribution=[hash[sr_hdemo_sk]])
            :              :                 +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], metadata=[]]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
            :              +- Exchange(distribution=[broadcast])
            :                 +- Calc(select=[sr_item_sk_node_22, t_time_sk_node_23, t_time_id_node_23, t_time_node_23, t_hour_node_23, t_minute_node_23, t_second_node_23, t_am_pm_node_23, t_shift_node_23, t_sub_shift_node_23, t_meal_time_node_23])
            :                    +- HashJoin(joinType=[InnerJoin], where=[=(sr_item_sk_node_22, t_hour_node_230)], select=[sr_item_sk_node_22, t_time_sk_node_23, t_time_id_node_23, t_time_node_23, t_hour_node_23, t_minute_node_23, t_second_node_23, t_am_pm_node_23, t_shift_node_23, t_sub_shift_node_23, t_meal_time_node_23, t_hour_node_230], isBroadcast=[true], build=[left])
            :                       :- Exchange(distribution=[broadcast])
            :                       :  +- HashAggregate(isMerge=[true], groupBy=[sr_item_sk_node_22], select=[sr_item_sk_node_22])
            :                       :     +- Exchange(distribution=[hash[sr_item_sk_node_22]])
            :                       :        +- LocalHashAggregate(groupBy=[sr_item_sk_node_22], select=[sr_item_sk_node_22])
            :                       :           +- Calc(select=[EXPR$0 AS sr_item_sk_node_22])
            :                       :              +- PythonGroupAggregate(groupBy=[sr_fee], select=[sr_fee, preloaded_udf_agg(sr_item_sk) AS EXPR$0])
            :                       :                 +- Sort(orderBy=[sr_fee ASC])
            :                       :                    +- Exchange(distribution=[hash[sr_fee]])
            :                       :                       +- Calc(select=[sr_item_sk, sr_fee])
            :                       :                          +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], metadata=[]]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
            :                       +- Calc(select=[t_time_sk AS t_time_sk_node_23, t_time_id AS t_time_id_node_23, t_time AS t_time_node_23, t_hour AS t_hour_node_23, t_minute AS t_minute_node_23, t_second AS t_second_node_23, t_am_pm AS t_am_pm_node_23, t_shift AS t_shift_node_23, t_sub_shift AS t_sub_shift_node_23, t_meal_time AS t_meal_time_node_23, CAST(t_hour AS BIGINT) AS t_hour_node_230], where=[>=(CHAR_LENGTH(t_time_id), 5)])
            :                          +- TableSourceScan(table=[[default_catalog, default_database, time_dim, filter=[>=(CHAR_LENGTH(t_time_id), 5)]]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
            +- Calc(select=[5Rdqb AS qZVlS, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24, 'hello' AS _c4])
               +- HashAggregate(isMerge=[true], groupBy=[5Rdqb, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24], select=[5Rdqb, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24])
                  +- Exchange(distribution=[hash[5Rdqb, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24]])
                     +- LocalHashAggregate(groupBy=[5Rdqb, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24], select=[5Rdqb, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24])
                        +- Calc(select=[inv_date_sk AS 5Rdqb, inv_item_sk AS inv_item_sk_node_24, inv_warehouse_sk AS inv_warehouse_sk_node_24, inv_quantity_on_hand AS inv_quantity_on_hand_node_24], where=[<=(inv_item_sk, 16)])
                           +- TableSourceScan(table=[[default_catalog, default_database, inventory, filter=[<=(inv_item_sk, 16)]]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS inv_quantity_on_hand_node_24])
+- HashAggregate(isMerge=[true], groupBy=[inv_warehouse_sk_node_24], select=[inv_warehouse_sk_node_24, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[hash[inv_warehouse_sk_node_24]])
      +- LocalHashAggregate(groupBy=[inv_warehouse_sk_node_24], select=[inv_warehouse_sk_node_24, Partial_COUNT(inv_quantity_on_hand_node_24) AS count$0])
         +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(d_qoy_node_20 = inv_item_sk_node_24)], select=[d_qoy_node_20, qZVlS, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24, _c4], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- Calc(select=[5Rdqb AS qZVlS, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24, 'hello' AS _c4])\
   +- HashAggregate(isMerge=[true], groupBy=[5Rdqb, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24], select=[5Rdqb, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24])\
      +- [#2] Exchange(distribution=[hash[5Rdqb, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24]])\
])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[EXPR$0 AS d_qoy_node_20])
            :     +- HashAggregate(isMerge=[false], groupBy=[d_holiday], select=[d_holiday, MIN(d_qoy) AS EXPR$0])
            :        +- Exchange(distribution=[hash[d_holiday]])
            :           +- NestedLoopJoin(joinType=[InnerJoin], where=[(t_meal_time_node_23 = d_current_day)], select=[w_warehouse_sk_node_18, w_warehouse_id_node_18, w_warehouse_name_node_18, w_warehouse_sq_ft_node_18, w_street_number_node_18, w_street_name_node_18, w_street_type_node_18, w_suite_number_node_18, w_city_node_18, w_county_node_18, w_state_node_18, w_zip_node_18, w_country_node_18, w_gmt_offset_node_18, cs_sold_date_sk_node_19, cs_sold_time_sk_node_19, cs_ship_date_sk_node_19, cs_bill_customer_sk_node_19, cs_bill_cdemo_sk_node_19, cs_bill_hdemo_sk_node_19, cs_bill_addr_sk_node_19, cs_ship_customer_sk_node_19, cs_ship_cdemo_sk_node_19, cs_ship_hdemo_sk_node_19, cs_ship_addr_sk_node_19, cs_call_center_sk_node_19, cs_catalog_page_sk_node_19, cs_ship_mode_sk_node_19, cs_warehouse_sk_node_19, cs_item_sk_node_19, cs_promo_sk_node_19, cs_order_number_node_19, cs_quantity_node_19, cs_wholesale_cost_node_19, cs_list_price_node_19, cs_sales_price_node_19, cs_ext_discount_amt_node_19, cs_ext_sales_price_node_19, cs_ext_wholesale_cost_node_19, cs_ext_list_price_node_19, cs_ext_tax_node_19, cs_coupon_amt_node_19, cs_ext_ship_cost_node_19, cs_net_paid_node_19, cs_net_paid_inc_tax_node_19, cs_net_paid_inc_ship_node_19, cs_net_paid_inc_ship_tax_node_19, cs_net_profit_node_19, d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, sr_item_sk_node_22, t_time_sk_node_23, t_time_id_node_23, t_time_node_23, t_hour_node_23, t_minute_node_23, t_second_node_23, t_am_pm_node_23, t_shift_node_23, t_sub_shift_node_23, t_meal_time_node_23], build=[right])
            :              :- Sort(orderBy=[sr_store_credit ASC])
            :              :  +- Exchange(distribution=[single])
            :              :     +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(w_suite_number_node_18 = d_quarter_name)], select=[w_warehouse_sk_node_18, w_warehouse_id_node_18, w_warehouse_name_node_18, w_warehouse_sq_ft_node_18, w_street_number_node_18, w_street_name_node_18, w_street_type_node_18, w_suite_number_node_18, w_city_node_18, w_county_node_18, w_state_node_18, w_zip_node_18, w_country_node_18, w_gmt_offset_node_18, cs_sold_date_sk_node_19, cs_sold_time_sk_node_19, cs_ship_date_sk_node_19, cs_bill_customer_sk_node_19, cs_bill_cdemo_sk_node_19, cs_bill_hdemo_sk_node_19, cs_bill_addr_sk_node_19, cs_ship_customer_sk_node_19, cs_ship_cdemo_sk_node_19, cs_ship_hdemo_sk_node_19, cs_ship_addr_sk_node_19, cs_call_center_sk_node_19, cs_catalog_page_sk_node_19, cs_ship_mode_sk_node_19, cs_warehouse_sk_node_19, cs_item_sk_node_19, cs_promo_sk_node_19, cs_order_number_node_19, cs_quantity_node_19, cs_wholesale_cost_node_19, cs_list_price_node_19, cs_sales_price_node_19, cs_ext_discount_amt_node_19, cs_ext_sales_price_node_19, cs_ext_wholesale_cost_node_19, cs_ext_list_price_node_19, cs_ext_tax_node_19, cs_coupon_amt_node_19, cs_ext_ship_cost_node_19, cs_net_paid_node_19, cs_net_paid_inc_tax_node_19, cs_net_paid_inc_ship_node_19, cs_net_paid_inc_ship_tax_node_19, cs_net_profit_node_19, d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], build=[right])
            :              :        :- Exchange(distribution=[hash[w_suite_number_node_18]])
            :              :        :  +- Calc(select=[w_warehouse_sk_node_18, w_warehouse_id_node_18, w_warehouse_name_node_18, w_warehouse_sq_ft_node_18, w_street_number_node_18, w_street_name_node_18, w_street_type_node_18, w_suite_number_node_18, w_city_node_18, w_county_node_18, w_state_node_18, w_zip_node_18, w_country_node_18, w_gmt_offset_node_18, cs_sold_date_sk AS cs_sold_date_sk_node_19, cs_sold_time_sk AS cs_sold_time_sk_node_19, cs_ship_date_sk AS cs_ship_date_sk_node_19, cs_bill_customer_sk AS cs_bill_customer_sk_node_19, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_19, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_19, cs_bill_addr_sk AS cs_bill_addr_sk_node_19, cs_ship_customer_sk AS cs_ship_customer_sk_node_19, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_19, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_19, cs_ship_addr_sk AS cs_ship_addr_sk_node_19, cs_call_center_sk AS cs_call_center_sk_node_19, cs_catalog_page_sk AS cs_catalog_page_sk_node_19, cs_ship_mode_sk AS cs_ship_mode_sk_node_19, cs_warehouse_sk AS cs_warehouse_sk_node_19, cs_item_sk AS cs_item_sk_node_19, cs_promo_sk AS cs_promo_sk_node_19, cs_order_number AS cs_order_number_node_19, cs_quantity AS cs_quantity_node_19, cs_wholesale_cost AS cs_wholesale_cost_node_19, cs_list_price AS cs_list_price_node_19, cs_sales_price AS cs_sales_price_node_19, cs_ext_discount_amt AS cs_ext_discount_amt_node_19, cs_ext_sales_price AS cs_ext_sales_price_node_19, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_19, cs_ext_list_price AS cs_ext_list_price_node_19, cs_ext_tax AS cs_ext_tax_node_19, cs_coupon_amt AS cs_coupon_amt_node_19, cs_ext_ship_cost AS cs_ext_ship_cost_node_19, cs_net_paid AS cs_net_paid_node_19, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_19, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_19, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_19, cs_net_profit AS cs_net_profit_node_19])
            :              :        :     +- MultipleInput(readOrder=[0,1], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(w_gmt_offset_node_180 = cs_ext_list_price)], select=[w_warehouse_sk_node_18, w_warehouse_id_node_18, w_warehouse_name_node_18, w_warehouse_sq_ft_node_18, w_street_number_node_18, w_street_name_node_18, w_street_type_node_18, w_suite_number_node_18, w_city_node_18, w_county_node_18, w_state_node_18, w_zip_node_18, w_country_node_18, w_gmt_offset_node_18, w_gmt_offset_node_180, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])\
])
            :              :        :        :- Exchange(distribution=[broadcast])
            :              :        :        :  +- Calc(select=[w_warehouse_sk AS w_warehouse_sk_node_18, w_warehouse_id AS w_warehouse_id_node_18, w_warehouse_name AS w_warehouse_name_node_18, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_18, w_street_number AS w_street_number_node_18, w_street_name AS w_street_name_node_18, w_street_type AS w_street_type_node_18, w_suite_number AS w_suite_number_node_18, w_city AS w_city_node_18, w_county AS w_county_node_18, w_state AS w_state_node_18, w_zip AS w_zip_node_18, w_country AS w_country_node_18, w_gmt_offset AS w_gmt_offset_node_18, CAST(w_gmt_offset AS DECIMAL(7, 2)) AS w_gmt_offset_node_180])
            :              :        :        :     +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
            :              :        :        +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
            :              :        +- Exchange(distribution=[hash[d_quarter_name]])
            :              :           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(d_dow = sr_hdemo_sk)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], build=[left])
            :              :              :- Exchange(distribution=[hash[d_dow]])
            :              :              :  +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
            :              :              +- Exchange(distribution=[hash[sr_hdemo_sk]])
            :              :                 +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], metadata=[]]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])(reuse_id=[1])
            :              +- Exchange(distribution=[broadcast])
            :                 +- Calc(select=[sr_item_sk_node_22, t_time_sk_node_23, t_time_id_node_23, t_time_node_23, t_hour_node_23, t_minute_node_23, t_second_node_23, t_am_pm_node_23, t_shift_node_23, t_sub_shift_node_23, t_meal_time_node_23])
            :                    +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(sr_item_sk_node_22 = t_hour_node_230)], select=[sr_item_sk_node_22, t_time_sk_node_23, t_time_id_node_23, t_time_node_23, t_hour_node_23, t_minute_node_23, t_second_node_23, t_am_pm_node_23, t_shift_node_23, t_sub_shift_node_23, t_meal_time_node_23, t_hour_node_230], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- Calc(select=[t_time_sk AS t_time_sk_node_23, t_time_id AS t_time_id_node_23, t_time AS t_time_node_23, t_hour AS t_hour_node_23, t_minute AS t_minute_node_23, t_second AS t_second_node_23, t_am_pm AS t_am_pm_node_23, t_shift AS t_shift_node_23, t_sub_shift AS t_sub_shift_node_23, t_meal_time AS t_meal_time_node_23, CAST(t_hour AS BIGINT) AS t_hour_node_230], where=[(CHAR_LENGTH(t_time_id) >= 5)])\
   +- [#2] TableSourceScan(table=[[default_catalog, default_database, time_dim, filter=[>=(CHAR_LENGTH(t_time_id), 5)]]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])\
])
            :                       :- Exchange(distribution=[broadcast])
            :                       :  +- HashAggregate(isMerge=[true], groupBy=[sr_item_sk_node_22], select=[sr_item_sk_node_22])
            :                       :     +- Exchange(distribution=[hash[sr_item_sk_node_22]])
            :                       :        +- LocalHashAggregate(groupBy=[sr_item_sk_node_22], select=[sr_item_sk_node_22])
            :                       :           +- Calc(select=[EXPR$0 AS sr_item_sk_node_22])
            :                       :              +- PythonGroupAggregate(groupBy=[sr_fee], select=[sr_fee, preloaded_udf_agg(sr_item_sk) AS EXPR$0])
            :                       :                 +- Exchange(distribution=[keep_input_as_is[hash[sr_fee]]])
            :                       :                    +- Sort(orderBy=[sr_fee ASC])
            :                       :                       +- Exchange(distribution=[hash[sr_fee]])
            :                       :                          +- Calc(select=[sr_item_sk, sr_fee])
            :                       :                             +- Reused(reference_id=[1])
            :                       +- TableSourceScan(table=[[default_catalog, default_database, time_dim, filter=[>=(CHAR_LENGTH(t_time_id), 5)]]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
            +- Exchange(distribution=[hash[5Rdqb, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24]])
               +- LocalHashAggregate(groupBy=[5Rdqb, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24], select=[5Rdqb, inv_item_sk_node_24, inv_warehouse_sk_node_24, inv_quantity_on_hand_node_24])
                  +- Calc(select=[inv_date_sk AS 5Rdqb, inv_item_sk AS inv_item_sk_node_24, inv_warehouse_sk AS inv_warehouse_sk_node_24, inv_quantity_on_hand AS inv_quantity_on_hand_node_24], where=[(inv_item_sk <= 16)])
                     +- TableSourceScan(table=[[default_catalog, default_database, inventory, filter=[<=(inv_item_sk, 16)]]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o260764538.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#527052820:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[94](input=RelSubset#527052818,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[94]), rel#527052817:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[94](input=RelSubset#527052816,groupBy=d_holiday, d_current_day,select=d_holiday, d_current_day, Partial_MIN(d_qoy) AS min$0)]
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