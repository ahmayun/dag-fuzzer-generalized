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


def preloaded_aggregation(values: pd.Series) -> int:
    return values.nunique()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_9 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_13 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_12 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_11 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_6 = autonode_9.filter(col('d_dom_node_9') >= -46)
autonode_8 = autonode_12.join(autonode_13, col('ss_hdemo_sk_node_12') == col('c_birth_day_node_13'))
autonode_7 = autonode_10.join(autonode_11, col('w_gmt_offset_node_11') == col('sr_store_credit_node_10'))
autonode_5 = autonode_8.order_by(col('ss_list_price_node_12'))
autonode_4 = autonode_6.join(autonode_7, col('d_quarter_name_node_9') == col('w_warehouse_name_node_11'))
autonode_3 = autonode_4.join(autonode_5, col('ss_ext_discount_amt_node_12') == col('sr_store_credit_node_10'))
autonode_2 = autonode_3.group_by(col('ss_wholesale_cost_node_12')).select(col('ss_list_price_node_12').max.alias('ss_list_price_node_12'))
autonode_1 = autonode_2.distinct()
sink = autonode_1.limit(30)
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
LogicalSort(fetch=[30])
+- LogicalAggregate(group=[{0}])
   +- LogicalProject(ss_list_price_node_12=[$1])
      +- LogicalAggregate(group=[{73}], EXPR$0=[MAX($74)])
         +- LogicalJoin(condition=[=($76, $46)], joinType=[inner])
            :- LogicalJoin(condition=[=($15, $50)], joinType=[inner])
            :  :- LogicalFilter(condition=[>=($9, -46)])
            :  :  +- LogicalProject(d_date_sk_node_9=[$0], d_date_id_node_9=[$1], d_date_node_9=[$2], d_month_seq_node_9=[$3], d_week_seq_node_9=[$4], d_quarter_seq_node_9=[$5], d_year_node_9=[$6], d_dow_node_9=[$7], d_moy_node_9=[$8], d_dom_node_9=[$9], d_qoy_node_9=[$10], d_fy_year_node_9=[$11], d_fy_quarter_seq_node_9=[$12], d_fy_week_seq_node_9=[$13], d_day_name_node_9=[$14], d_quarter_name_node_9=[$15], d_holiday_node_9=[$16], d_weekend_node_9=[$17], d_following_holiday_node_9=[$18], d_first_dom_node_9=[$19], d_last_dom_node_9=[$20], d_same_day_ly_node_9=[$21], d_same_day_lq_node_9=[$22], d_current_day_node_9=[$23], d_current_week_node_9=[$24], d_current_month_node_9=[$25], d_current_quarter_node_9=[$26], d_current_year_node_9=[$27])
            :  :     +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
            :  +- LogicalJoin(condition=[=($33, $18)], joinType=[inner])
            :     :- LogicalProject(sr_returned_date_sk_node_10=[$0], sr_return_time_sk_node_10=[$1], sr_item_sk_node_10=[$2], sr_customer_sk_node_10=[$3], sr_cdemo_sk_node_10=[$4], sr_hdemo_sk_node_10=[$5], sr_addr_sk_node_10=[$6], sr_store_sk_node_10=[$7], sr_reason_sk_node_10=[$8], sr_ticket_number_node_10=[$9], sr_return_quantity_node_10=[$10], sr_return_amt_node_10=[$11], sr_return_tax_node_10=[$12], sr_return_amt_inc_tax_node_10=[$13], sr_fee_node_10=[$14], sr_return_ship_cost_node_10=[$15], sr_refunded_cash_node_10=[$16], sr_reversed_charge_node_10=[$17], sr_store_credit_node_10=[$18], sr_net_loss_node_10=[$19])
            :     :  +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
            :     +- LogicalProject(w_warehouse_sk_node_11=[$0], w_warehouse_id_node_11=[$1], w_warehouse_name_node_11=[$2], w_warehouse_sq_ft_node_11=[$3], w_street_number_node_11=[$4], w_street_name_node_11=[$5], w_street_type_node_11=[$6], w_suite_number_node_11=[$7], w_city_node_11=[$8], w_county_node_11=[$9], w_state_node_11=[$10], w_zip_node_11=[$11], w_country_node_11=[$12], w_gmt_offset_node_11=[$13])
            :        +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])
            +- LogicalSort(sort0=[$12], dir0=[ASC])
               +- LogicalJoin(condition=[=($5, $34)], joinType=[inner])
                  :- LogicalProject(ss_sold_date_sk_node_12=[$0], ss_sold_time_sk_node_12=[$1], ss_item_sk_node_12=[$2], ss_customer_sk_node_12=[$3], ss_cdemo_sk_node_12=[$4], ss_hdemo_sk_node_12=[$5], ss_addr_sk_node_12=[$6], ss_store_sk_node_12=[$7], ss_promo_sk_node_12=[$8], ss_ticket_number_node_12=[$9], ss_quantity_node_12=[$10], ss_wholesale_cost_node_12=[$11], ss_list_price_node_12=[$12], ss_sales_price_node_12=[$13], ss_ext_discount_amt_node_12=[$14], ss_ext_sales_price_node_12=[$15], ss_ext_wholesale_cost_node_12=[$16], ss_ext_list_price_node_12=[$17], ss_ext_tax_node_12=[$18], ss_coupon_amt_node_12=[$19], ss_net_paid_node_12=[$20], ss_net_paid_inc_tax_node_12=[$21], ss_net_profit_node_12=[$22])
                  :  +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
                  +- LogicalProject(c_customer_sk_node_13=[$0], c_customer_id_node_13=[$1], c_current_cdemo_sk_node_13=[$2], c_current_hdemo_sk_node_13=[$3], c_current_addr_sk_node_13=[$4], c_first_shipto_date_sk_node_13=[$5], c_first_sales_date_sk_node_13=[$6], c_salutation_node_13=[$7], c_first_name_node_13=[$8], c_last_name_node_13=[$9], c_preferred_cust_flag_node_13=[$10], c_birth_day_node_13=[$11], c_birth_month_node_13=[$12], c_birth_year_node_13=[$13], c_birth_country_node_13=[$14], c_login_node_13=[$15], c_email_address_node_13=[$16], c_last_review_date_node_13=[$17])
                     +- LogicalTableScan(table=[[default_catalog, default_database, customer]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[30], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[30], global=[false])
      +- HashAggregate(isMerge=[true], groupBy=[ss_list_price_node_12], select=[ss_list_price_node_12])
         +- Exchange(distribution=[hash[ss_list_price_node_12]])
            +- LocalHashAggregate(groupBy=[ss_list_price_node_12], select=[ss_list_price_node_12])
               +- Calc(select=[EXPR$0 AS ss_list_price_node_12])
                  +- HashAggregate(isMerge=[false], groupBy=[ss_wholesale_cost], select=[ss_wholesale_cost, MAX(ss_list_price) AS EXPR$0])
                     +- Exchange(distribution=[hash[ss_wholesale_cost]])
                        +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_ext_discount_amt, sr_store_credit_node_10)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10, w_warehouse_sk_node_11, w_warehouse_id_node_11, w_warehouse_name_node_11, w_warehouse_sq_ft_node_11, w_street_number_node_11, w_street_name_node_11, w_street_type_node_11, w_suite_number_node_11, w_city_node_11, w_county_node_11, w_state_node_11, w_zip_node_11, w_country_node_11, w_gmt_offset_node_11, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[left])
                           :- Exchange(distribution=[broadcast])
                           :  +- HashJoin(joinType=[InnerJoin], where=[=(d_quarter_name, w_warehouse_name_node_11)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10, w_warehouse_sk_node_11, w_warehouse_id_node_11, w_warehouse_name_node_11, w_warehouse_sq_ft_node_11, w_street_number_node_11, w_street_name_node_11, w_street_type_node_11, w_suite_number_node_11, w_city_node_11, w_county_node_11, w_state_node_11, w_zip_node_11, w_country_node_11, w_gmt_offset_node_11], build=[left])
                           :     :- Exchange(distribution=[hash[d_quarter_name]])
                           :     :  +- Calc(select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year], where=[>=(d_dom, -46)])
                           :     :     +- TableSourceScan(table=[[default_catalog, default_database, date_dim, filter=[>=(d_dom, -46)]]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
                           :     +- Exchange(distribution=[hash[w_warehouse_name_node_11]])
                           :        +- Calc(select=[sr_returned_date_sk AS sr_returned_date_sk_node_10, sr_return_time_sk AS sr_return_time_sk_node_10, sr_item_sk AS sr_item_sk_node_10, sr_customer_sk AS sr_customer_sk_node_10, sr_cdemo_sk AS sr_cdemo_sk_node_10, sr_hdemo_sk AS sr_hdemo_sk_node_10, sr_addr_sk AS sr_addr_sk_node_10, sr_store_sk AS sr_store_sk_node_10, sr_reason_sk AS sr_reason_sk_node_10, sr_ticket_number AS sr_ticket_number_node_10, sr_return_quantity AS sr_return_quantity_node_10, sr_return_amt AS sr_return_amt_node_10, sr_return_tax AS sr_return_tax_node_10, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_10, sr_fee AS sr_fee_node_10, sr_return_ship_cost AS sr_return_ship_cost_node_10, sr_refunded_cash AS sr_refunded_cash_node_10, sr_reversed_charge AS sr_reversed_charge_node_10, sr_store_credit AS sr_store_credit_node_10, sr_net_loss AS sr_net_loss_node_10, w_warehouse_sk_node_11, w_warehouse_id_node_11, w_warehouse_name_node_11, w_warehouse_sq_ft_node_11, w_street_number_node_11, w_street_name_node_11, w_street_type_node_11, w_suite_number_node_11, w_city_node_11, w_county_node_11, w_state_node_11, w_zip_node_11, w_country_node_11, w_gmt_offset_node_11])
                           :           +- NestedLoopJoin(joinType=[InnerJoin], where=[=(w_gmt_offset_node_110, sr_store_credit)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, w_warehouse_sk_node_11, w_warehouse_id_node_11, w_warehouse_name_node_11, w_warehouse_sq_ft_node_11, w_street_number_node_11, w_street_name_node_11, w_street_type_node_11, w_suite_number_node_11, w_city_node_11, w_county_node_11, w_state_node_11, w_zip_node_11, w_country_node_11, w_gmt_offset_node_11, w_gmt_offset_node_110], build=[right])
                           :              :- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                           :              +- Exchange(distribution=[broadcast])
                           :                 +- Calc(select=[w_warehouse_sk AS w_warehouse_sk_node_11, w_warehouse_id AS w_warehouse_id_node_11, w_warehouse_name AS w_warehouse_name_node_11, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_11, w_street_number AS w_street_number_node_11, w_street_name AS w_street_name_node_11, w_street_type AS w_street_type_node_11, w_suite_number AS w_suite_number_node_11, w_city AS w_city_node_11, w_county AS w_county_node_11, w_state AS w_state_node_11, w_zip AS w_zip_node_11, w_country AS w_country_node_11, w_gmt_offset AS w_gmt_offset_node_11, CAST(w_gmt_offset AS DECIMAL(7, 2)) AS w_gmt_offset_node_110])
                           :                    +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
                           +- Sort(orderBy=[ss_list_price ASC])
                              +- Exchange(distribution=[single])
                                 +- HashJoin(joinType=[InnerJoin], where=[=(ss_hdemo_sk, c_birth_day)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[right])
                                    :- Exchange(distribution=[hash[ss_hdemo_sk]])
                                    :  +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                                    +- Exchange(distribution=[hash[c_birth_day]])
                                       +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[30], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[30], global=[false])
      +- HashAggregate(isMerge=[true], groupBy=[ss_list_price_node_12], select=[ss_list_price_node_12])
         +- Exchange(distribution=[hash[ss_list_price_node_12]])
            +- LocalHashAggregate(groupBy=[ss_list_price_node_12], select=[ss_list_price_node_12])
               +- Calc(select=[EXPR$0 AS ss_list_price_node_12])
                  +- HashAggregate(isMerge=[false], groupBy=[ss_wholesale_cost], select=[ss_wholesale_cost, MAX(ss_list_price) AS EXPR$0])
                     +- Exchange(distribution=[hash[ss_wholesale_cost]])
                        +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_ext_discount_amt = sr_store_credit_node_10)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10, w_warehouse_sk_node_11, w_warehouse_id_node_11, w_warehouse_name_node_11, w_warehouse_sq_ft_node_11, w_street_number_node_11, w_street_name_node_11, w_street_type_node_11, w_suite_number_node_11, w_city_node_11, w_county_node_11, w_state_node_11, w_zip_node_11, w_country_node_11, w_gmt_offset_node_11, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[left])
                           :- Exchange(distribution=[broadcast])
                           :  +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(d_quarter_name = w_warehouse_name_node_11)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10, w_warehouse_sk_node_11, w_warehouse_id_node_11, w_warehouse_name_node_11, w_warehouse_sq_ft_node_11, w_street_number_node_11, w_street_name_node_11, w_street_type_node_11, w_suite_number_node_11, w_city_node_11, w_county_node_11, w_state_node_11, w_zip_node_11, w_country_node_11, w_gmt_offset_node_11], build=[left])
                           :     :- Exchange(distribution=[hash[d_quarter_name]])
                           :     :  +- Calc(select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year], where=[(d_dom >= -46)])
                           :     :     +- TableSourceScan(table=[[default_catalog, default_database, date_dim, filter=[>=(d_dom, -46)]]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
                           :     +- Exchange(distribution=[hash[w_warehouse_name_node_11]])
                           :        +- Calc(select=[sr_returned_date_sk AS sr_returned_date_sk_node_10, sr_return_time_sk AS sr_return_time_sk_node_10, sr_item_sk AS sr_item_sk_node_10, sr_customer_sk AS sr_customer_sk_node_10, sr_cdemo_sk AS sr_cdemo_sk_node_10, sr_hdemo_sk AS sr_hdemo_sk_node_10, sr_addr_sk AS sr_addr_sk_node_10, sr_store_sk AS sr_store_sk_node_10, sr_reason_sk AS sr_reason_sk_node_10, sr_ticket_number AS sr_ticket_number_node_10, sr_return_quantity AS sr_return_quantity_node_10, sr_return_amt AS sr_return_amt_node_10, sr_return_tax AS sr_return_tax_node_10, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_10, sr_fee AS sr_fee_node_10, sr_return_ship_cost AS sr_return_ship_cost_node_10, sr_refunded_cash AS sr_refunded_cash_node_10, sr_reversed_charge AS sr_reversed_charge_node_10, sr_store_credit AS sr_store_credit_node_10, sr_net_loss AS sr_net_loss_node_10, w_warehouse_sk_node_11, w_warehouse_id_node_11, w_warehouse_name_node_11, w_warehouse_sq_ft_node_11, w_street_number_node_11, w_street_name_node_11, w_street_type_node_11, w_suite_number_node_11, w_city_node_11, w_county_node_11, w_state_node_11, w_zip_node_11, w_country_node_11, w_gmt_offset_node_11])
                           :           +- MultipleInput(readOrder=[1,0], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(w_gmt_offset_node_110 = sr_store_credit)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, w_warehouse_sk_node_11, w_warehouse_id_node_11, w_warehouse_name_node_11, w_warehouse_sq_ft_node_11, w_street_number_node_11, w_street_name_node_11, w_street_type_node_11, w_suite_number_node_11, w_city_node_11, w_county_node_11, w_state_node_11, w_zip_node_11, w_country_node_11, w_gmt_offset_node_11, w_gmt_offset_node_110], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])\
+- [#2] Exchange(distribution=[broadcast])\
])
                           :              :- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                           :              +- Exchange(distribution=[broadcast])
                           :                 +- Calc(select=[w_warehouse_sk AS w_warehouse_sk_node_11, w_warehouse_id AS w_warehouse_id_node_11, w_warehouse_name AS w_warehouse_name_node_11, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_11, w_street_number AS w_street_number_node_11, w_street_name AS w_street_name_node_11, w_street_type AS w_street_type_node_11, w_suite_number AS w_suite_number_node_11, w_city AS w_city_node_11, w_county AS w_county_node_11, w_state AS w_state_node_11, w_zip AS w_zip_node_11, w_country AS w_country_node_11, w_gmt_offset AS w_gmt_offset_node_11, CAST(w_gmt_offset AS DECIMAL(7, 2)) AS w_gmt_offset_node_110])
                           :                    +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
                           +- Sort(orderBy=[ss_list_price ASC])
                              +- Exchange(distribution=[single])
                                 +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ss_hdemo_sk = c_birth_day)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[right])
                                    :- Exchange(distribution=[hash[ss_hdemo_sk]])
                                    :  +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                                    +- Exchange(distribution=[hash[c_birth_day]])
                                       +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o68657482.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#137590869:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[12](input=RelSubset#137590867,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[12]), rel#137590866:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[12](input=RelSubset#137590865,groupBy=ss_wholesale_cost, ss_ext_discount_amt,select=ss_wholesale_cost, ss_ext_discount_amt, Partial_MAX(ss_list_price) AS max$0)]
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