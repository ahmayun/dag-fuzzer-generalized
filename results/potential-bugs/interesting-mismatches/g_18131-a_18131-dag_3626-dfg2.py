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
    return values.iloc[0] if len(values) > 0 else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_9 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_8 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_7 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_11 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_5 = autonode_9.join(autonode_10, col('c_current_hdemo_sk_node_10') == col('hd_demo_sk_node_9'))
autonode_4 = autonode_7.join(autonode_8, col('ib_income_band_sk_node_7') == col('w_warehouse_sk_node_8'))
autonode_6 = autonode_11.order_by(col('ss_coupon_amt_node_11'))
autonode_2 = autonode_4.join(autonode_5, col('w_warehouse_sq_ft_node_8') == col('hd_vehicle_count_node_9'))
autonode_3 = autonode_6.alias('jOU12')
autonode_1 = autonode_2.join(autonode_3, col('w_gmt_offset_node_8') == col('ss_net_paid_node_11'))
sink = autonode_1.group_by(col('ss_ext_discount_amt_node_11')).select(col('ss_sales_price_node_11').max.alias('ss_sales_price_node_11'))
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
LogicalProject(ss_sales_price_node_11=[$1])
+- LogicalAggregate(group=[{54}], EXPR$0=[MAX($53)])
   +- LogicalJoin(condition=[=($16, $60)], joinType=[inner])
      :- LogicalJoin(condition=[=($6, $21)], joinType=[inner])
      :  :- LogicalJoin(condition=[=($0, $3)], joinType=[inner])
      :  :  :- LogicalProject(ib_income_band_sk_node_7=[$0], ib_lower_bound_node_7=[$1], ib_upper_bound_node_7=[$2])
      :  :  :  +- LogicalTableScan(table=[[default_catalog, default_database, income_band]])
      :  :  +- LogicalProject(w_warehouse_sk_node_8=[$0], w_warehouse_id_node_8=[$1], w_warehouse_name_node_8=[$2], w_warehouse_sq_ft_node_8=[$3], w_street_number_node_8=[$4], w_street_name_node_8=[$5], w_street_type_node_8=[$6], w_suite_number_node_8=[$7], w_city_node_8=[$8], w_county_node_8=[$9], w_state_node_8=[$10], w_zip_node_8=[$11], w_country_node_8=[$12], w_gmt_offset_node_8=[$13])
      :  :     +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])
      :  +- LogicalJoin(condition=[=($8, $0)], joinType=[inner])
      :     :- LogicalProject(hd_demo_sk_node_9=[$0], hd_income_band_sk_node_9=[$1], hd_buy_potential_node_9=[$2], hd_dep_count_node_9=[$3], hd_vehicle_count_node_9=[$4])
      :     :  +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
      :     +- LogicalProject(c_customer_sk_node_10=[$0], c_customer_id_node_10=[$1], c_current_cdemo_sk_node_10=[$2], c_current_hdemo_sk_node_10=[$3], c_current_addr_sk_node_10=[$4], c_first_shipto_date_sk_node_10=[$5], c_first_sales_date_sk_node_10=[$6], c_salutation_node_10=[$7], c_first_name_node_10=[$8], c_last_name_node_10=[$9], c_preferred_cust_flag_node_10=[$10], c_birth_day_node_10=[$11], c_birth_month_node_10=[$12], c_birth_year_node_10=[$13], c_birth_country_node_10=[$14], c_login_node_10=[$15], c_email_address_node_10=[$16], c_last_review_date_node_10=[$17])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
      +- LogicalProject(jOU12=[AS($0, _UTF-16LE'jOU12')], ss_sold_time_sk_node_11=[$1], ss_item_sk_node_11=[$2], ss_customer_sk_node_11=[$3], ss_cdemo_sk_node_11=[$4], ss_hdemo_sk_node_11=[$5], ss_addr_sk_node_11=[$6], ss_store_sk_node_11=[$7], ss_promo_sk_node_11=[$8], ss_ticket_number_node_11=[$9], ss_quantity_node_11=[$10], ss_wholesale_cost_node_11=[$11], ss_list_price_node_11=[$12], ss_sales_price_node_11=[$13], ss_ext_discount_amt_node_11=[$14], ss_ext_sales_price_node_11=[$15], ss_ext_wholesale_cost_node_11=[$16], ss_ext_list_price_node_11=[$17], ss_ext_tax_node_11=[$18], ss_coupon_amt_node_11=[$19], ss_net_paid_node_11=[$20], ss_net_paid_inc_tax_node_11=[$21], ss_net_profit_node_11=[$22])
         +- LogicalSort(sort0=[$19], dir0=[ASC])
            +- LogicalProject(ss_sold_date_sk_node_11=[$0], ss_sold_time_sk_node_11=[$1], ss_item_sk_node_11=[$2], ss_customer_sk_node_11=[$3], ss_cdemo_sk_node_11=[$4], ss_hdemo_sk_node_11=[$5], ss_addr_sk_node_11=[$6], ss_store_sk_node_11=[$7], ss_promo_sk_node_11=[$8], ss_ticket_number_node_11=[$9], ss_quantity_node_11=[$10], ss_wholesale_cost_node_11=[$11], ss_list_price_node_11=[$12], ss_sales_price_node_11=[$13], ss_ext_discount_amt_node_11=[$14], ss_ext_sales_price_node_11=[$15], ss_ext_wholesale_cost_node_11=[$16], ss_ext_list_price_node_11=[$17], ss_ext_tax_node_11=[$18], ss_coupon_amt_node_11=[$19], ss_net_paid_node_11=[$20], ss_net_paid_inc_tax_node_11=[$21], ss_net_profit_node_11=[$22])
               +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ss_sales_price_node_11])
+- HashAggregate(isMerge=[false], groupBy=[ss_ext_discount_amt_node_11], select=[ss_ext_discount_amt_node_11, MAX(ss_sales_price_node_11) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_ext_discount_amt_node_11]])
      +- Calc(select=[ib_income_band_sk_node_7, ib_lower_bound_node_7, ib_upper_bound_node_7, w_warehouse_sk_node_8, w_warehouse_id_node_8, w_warehouse_name_node_8, w_warehouse_sq_ft_node_8, w_street_number_node_8, w_street_name_node_8, w_street_type_node_8, w_suite_number_node_8, w_city_node_8, w_county_node_8, w_state_node_8, w_zip_node_8, w_country_node_8, w_gmt_offset_node_8, hd_demo_sk_node_9, hd_income_band_sk_node_9, hd_buy_potential_node_9, hd_dep_count_node_9, hd_vehicle_count_node_9, c_customer_sk_node_10, c_customer_id_node_10, c_current_cdemo_sk_node_10, c_current_hdemo_sk_node_10, c_current_addr_sk_node_10, c_first_shipto_date_sk_node_10, c_first_sales_date_sk_node_10, c_salutation_node_10, c_first_name_node_10, c_last_name_node_10, c_preferred_cust_flag_node_10, c_birth_day_node_10, c_birth_month_node_10, c_birth_year_node_10, c_birth_country_node_10, c_login_node_10, c_email_address_node_10, c_last_review_date_node_10, jOU12, ss_sold_time_sk_node_11, ss_item_sk_node_11, ss_customer_sk_node_11, ss_cdemo_sk_node_11, ss_hdemo_sk_node_11, ss_addr_sk_node_11, ss_store_sk_node_11, ss_promo_sk_node_11, ss_ticket_number_node_11, ss_quantity_node_11, ss_wholesale_cost_node_11, ss_list_price_node_11, ss_sales_price_node_11, ss_ext_discount_amt_node_11, ss_ext_sales_price_node_11, ss_ext_wholesale_cost_node_11, ss_ext_list_price_node_11, ss_ext_tax_node_11, ss_coupon_amt_node_11, ss_net_paid_node_11, ss_net_paid_inc_tax_node_11, ss_net_profit_node_11])
         +- HashJoin(joinType=[InnerJoin], where=[=(w_gmt_offset_node_80, ss_net_paid_node_11)], select=[ib_income_band_sk_node_7, ib_lower_bound_node_7, ib_upper_bound_node_7, w_warehouse_sk_node_8, w_warehouse_id_node_8, w_warehouse_name_node_8, w_warehouse_sq_ft_node_8, w_street_number_node_8, w_street_name_node_8, w_street_type_node_8, w_suite_number_node_8, w_city_node_8, w_county_node_8, w_state_node_8, w_zip_node_8, w_country_node_8, w_gmt_offset_node_8, hd_demo_sk_node_9, hd_income_band_sk_node_9, hd_buy_potential_node_9, hd_dep_count_node_9, hd_vehicle_count_node_9, c_customer_sk_node_10, c_customer_id_node_10, c_current_cdemo_sk_node_10, c_current_hdemo_sk_node_10, c_current_addr_sk_node_10, c_first_shipto_date_sk_node_10, c_first_sales_date_sk_node_10, c_salutation_node_10, c_first_name_node_10, c_last_name_node_10, c_preferred_cust_flag_node_10, c_birth_day_node_10, c_birth_month_node_10, c_birth_year_node_10, c_birth_country_node_10, c_login_node_10, c_email_address_node_10, c_last_review_date_node_10, w_gmt_offset_node_80, jOU12, ss_sold_time_sk_node_11, ss_item_sk_node_11, ss_customer_sk_node_11, ss_cdemo_sk_node_11, ss_hdemo_sk_node_11, ss_addr_sk_node_11, ss_store_sk_node_11, ss_promo_sk_node_11, ss_ticket_number_node_11, ss_quantity_node_11, ss_wholesale_cost_node_11, ss_list_price_node_11, ss_sales_price_node_11, ss_ext_discount_amt_node_11, ss_ext_sales_price_node_11, ss_ext_wholesale_cost_node_11, ss_ext_list_price_node_11, ss_ext_tax_node_11, ss_coupon_amt_node_11, ss_net_paid_node_11, ss_net_paid_inc_tax_node_11, ss_net_profit_node_11], build=[left])
            :- Exchange(distribution=[hash[w_gmt_offset_node_80]])
            :  +- Calc(select=[ib_income_band_sk AS ib_income_band_sk_node_7, ib_lower_bound AS ib_lower_bound_node_7, ib_upper_bound AS ib_upper_bound_node_7, w_warehouse_sk AS w_warehouse_sk_node_8, w_warehouse_id AS w_warehouse_id_node_8, w_warehouse_name AS w_warehouse_name_node_8, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_8, w_street_number AS w_street_number_node_8, w_street_name AS w_street_name_node_8, w_street_type AS w_street_type_node_8, w_suite_number AS w_suite_number_node_8, w_city AS w_city_node_8, w_county AS w_county_node_8, w_state AS w_state_node_8, w_zip AS w_zip_node_8, w_country AS w_country_node_8, w_gmt_offset AS w_gmt_offset_node_8, hd_demo_sk AS hd_demo_sk_node_9, hd_income_band_sk AS hd_income_band_sk_node_9, hd_buy_potential AS hd_buy_potential_node_9, hd_dep_count AS hd_dep_count_node_9, hd_vehicle_count AS hd_vehicle_count_node_9, c_customer_sk AS c_customer_sk_node_10, c_customer_id AS c_customer_id_node_10, c_current_cdemo_sk AS c_current_cdemo_sk_node_10, c_current_hdemo_sk AS c_current_hdemo_sk_node_10, c_current_addr_sk AS c_current_addr_sk_node_10, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_10, c_first_sales_date_sk AS c_first_sales_date_sk_node_10, c_salutation AS c_salutation_node_10, c_first_name AS c_first_name_node_10, c_last_name AS c_last_name_node_10, c_preferred_cust_flag AS c_preferred_cust_flag_node_10, c_birth_day AS c_birth_day_node_10, c_birth_month AS c_birth_month_node_10, c_birth_year AS c_birth_year_node_10, c_birth_country AS c_birth_country_node_10, c_login AS c_login_node_10, c_email_address AS c_email_address_node_10, c_last_review_date AS c_last_review_date_node_10, CAST(w_gmt_offset AS DECIMAL(7, 2)) AS w_gmt_offset_node_80])
            :     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(w_warehouse_sq_ft, hd_vehicle_count)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[left])
            :        :- Exchange(distribution=[broadcast])
            :        :  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ib_income_band_sk, w_warehouse_sk)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], build=[left])
            :        :     :- Exchange(distribution=[broadcast])
            :        :     :  +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
            :        :     +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
            :        +- HashJoin(joinType=[InnerJoin], where=[=(c_current_hdemo_sk, hd_demo_sk)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], isBroadcast=[true], build=[left])
            :           :- Exchange(distribution=[broadcast])
            :           :  +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
            :           +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
            +- Exchange(distribution=[hash[ss_net_paid_node_11]])
               +- Calc(select=[ss_sold_date_sk AS jOU12, ss_sold_time_sk AS ss_sold_time_sk_node_11, ss_item_sk AS ss_item_sk_node_11, ss_customer_sk AS ss_customer_sk_node_11, ss_cdemo_sk AS ss_cdemo_sk_node_11, ss_hdemo_sk AS ss_hdemo_sk_node_11, ss_addr_sk AS ss_addr_sk_node_11, ss_store_sk AS ss_store_sk_node_11, ss_promo_sk AS ss_promo_sk_node_11, ss_ticket_number AS ss_ticket_number_node_11, ss_quantity AS ss_quantity_node_11, ss_wholesale_cost AS ss_wholesale_cost_node_11, ss_list_price AS ss_list_price_node_11, ss_sales_price AS ss_sales_price_node_11, ss_ext_discount_amt AS ss_ext_discount_amt_node_11, ss_ext_sales_price AS ss_ext_sales_price_node_11, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_11, ss_ext_list_price AS ss_ext_list_price_node_11, ss_ext_tax AS ss_ext_tax_node_11, ss_coupon_amt AS ss_coupon_amt_node_11, ss_net_paid AS ss_net_paid_node_11, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_11, ss_net_profit AS ss_net_profit_node_11])
                  +- Sort(orderBy=[ss_coupon_amt ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ss_sales_price_node_11])
+- HashAggregate(isMerge=[false], groupBy=[ss_ext_discount_amt_node_11], select=[ss_ext_discount_amt_node_11, MAX(ss_sales_price_node_11) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_ext_discount_amt_node_11]])
      +- Calc(select=[ib_income_band_sk_node_7, ib_lower_bound_node_7, ib_upper_bound_node_7, w_warehouse_sk_node_8, w_warehouse_id_node_8, w_warehouse_name_node_8, w_warehouse_sq_ft_node_8, w_street_number_node_8, w_street_name_node_8, w_street_type_node_8, w_suite_number_node_8, w_city_node_8, w_county_node_8, w_state_node_8, w_zip_node_8, w_country_node_8, w_gmt_offset_node_8, hd_demo_sk_node_9, hd_income_band_sk_node_9, hd_buy_potential_node_9, hd_dep_count_node_9, hd_vehicle_count_node_9, c_customer_sk_node_10, c_customer_id_node_10, c_current_cdemo_sk_node_10, c_current_hdemo_sk_node_10, c_current_addr_sk_node_10, c_first_shipto_date_sk_node_10, c_first_sales_date_sk_node_10, c_salutation_node_10, c_first_name_node_10, c_last_name_node_10, c_preferred_cust_flag_node_10, c_birth_day_node_10, c_birth_month_node_10, c_birth_year_node_10, c_birth_country_node_10, c_login_node_10, c_email_address_node_10, c_last_review_date_node_10, jOU12, ss_sold_time_sk_node_11, ss_item_sk_node_11, ss_customer_sk_node_11, ss_cdemo_sk_node_11, ss_hdemo_sk_node_11, ss_addr_sk_node_11, ss_store_sk_node_11, ss_promo_sk_node_11, ss_ticket_number_node_11, ss_quantity_node_11, ss_wholesale_cost_node_11, ss_list_price_node_11, ss_sales_price_node_11, ss_ext_discount_amt_node_11, ss_ext_sales_price_node_11, ss_ext_wholesale_cost_node_11, ss_ext_list_price_node_11, ss_ext_tax_node_11, ss_coupon_amt_node_11, ss_net_paid_node_11, ss_net_paid_inc_tax_node_11, ss_net_profit_node_11])
         +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(w_gmt_offset_node_80 = ss_net_paid_node_11)], select=[ib_income_band_sk_node_7, ib_lower_bound_node_7, ib_upper_bound_node_7, w_warehouse_sk_node_8, w_warehouse_id_node_8, w_warehouse_name_node_8, w_warehouse_sq_ft_node_8, w_street_number_node_8, w_street_name_node_8, w_street_type_node_8, w_suite_number_node_8, w_city_node_8, w_county_node_8, w_state_node_8, w_zip_node_8, w_country_node_8, w_gmt_offset_node_8, hd_demo_sk_node_9, hd_income_band_sk_node_9, hd_buy_potential_node_9, hd_dep_count_node_9, hd_vehicle_count_node_9, c_customer_sk_node_10, c_customer_id_node_10, c_current_cdemo_sk_node_10, c_current_hdemo_sk_node_10, c_current_addr_sk_node_10, c_first_shipto_date_sk_node_10, c_first_sales_date_sk_node_10, c_salutation_node_10, c_first_name_node_10, c_last_name_node_10, c_preferred_cust_flag_node_10, c_birth_day_node_10, c_birth_month_node_10, c_birth_year_node_10, c_birth_country_node_10, c_login_node_10, c_email_address_node_10, c_last_review_date_node_10, w_gmt_offset_node_80, jOU12, ss_sold_time_sk_node_11, ss_item_sk_node_11, ss_customer_sk_node_11, ss_cdemo_sk_node_11, ss_hdemo_sk_node_11, ss_addr_sk_node_11, ss_store_sk_node_11, ss_promo_sk_node_11, ss_ticket_number_node_11, ss_quantity_node_11, ss_wholesale_cost_node_11, ss_list_price_node_11, ss_sales_price_node_11, ss_ext_discount_amt_node_11, ss_ext_sales_price_node_11, ss_ext_wholesale_cost_node_11, ss_ext_list_price_node_11, ss_ext_tax_node_11, ss_coupon_amt_node_11, ss_net_paid_node_11, ss_net_paid_inc_tax_node_11, ss_net_profit_node_11], build=[left])
            :- Exchange(distribution=[hash[w_gmt_offset_node_80]])
            :  +- Calc(select=[ib_income_band_sk AS ib_income_band_sk_node_7, ib_lower_bound AS ib_lower_bound_node_7, ib_upper_bound AS ib_upper_bound_node_7, w_warehouse_sk AS w_warehouse_sk_node_8, w_warehouse_id AS w_warehouse_id_node_8, w_warehouse_name AS w_warehouse_name_node_8, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_8, w_street_number AS w_street_number_node_8, w_street_name AS w_street_name_node_8, w_street_type AS w_street_type_node_8, w_suite_number AS w_suite_number_node_8, w_city AS w_city_node_8, w_county AS w_county_node_8, w_state AS w_state_node_8, w_zip AS w_zip_node_8, w_country AS w_country_node_8, w_gmt_offset AS w_gmt_offset_node_8, hd_demo_sk AS hd_demo_sk_node_9, hd_income_band_sk AS hd_income_band_sk_node_9, hd_buy_potential AS hd_buy_potential_node_9, hd_dep_count AS hd_dep_count_node_9, hd_vehicle_count AS hd_vehicle_count_node_9, c_customer_sk AS c_customer_sk_node_10, c_customer_id AS c_customer_id_node_10, c_current_cdemo_sk AS c_current_cdemo_sk_node_10, c_current_hdemo_sk AS c_current_hdemo_sk_node_10, c_current_addr_sk AS c_current_addr_sk_node_10, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_10, c_first_sales_date_sk AS c_first_sales_date_sk_node_10, c_salutation AS c_salutation_node_10, c_first_name AS c_first_name_node_10, c_last_name AS c_last_name_node_10, c_preferred_cust_flag AS c_preferred_cust_flag_node_10, c_birth_day AS c_birth_day_node_10, c_birth_month AS c_birth_month_node_10, c_birth_year AS c_birth_year_node_10, c_birth_country AS c_birth_country_node_10, c_login AS c_login_node_10, c_email_address AS c_email_address_node_10, c_last_review_date AS c_last_review_date_node_10, CAST(w_gmt_offset AS DECIMAL(7, 2)) AS w_gmt_offset_node_80])
            :     +- MultipleInput(readOrder=[0,0,1], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(w_warehouse_sq_ft = hd_vehicle_count)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- HashJoin(joinType=[InnerJoin], where=[(c_current_hdemo_sk = hd_demo_sk)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], isBroadcast=[true], build=[left])\
   :- [#2] Exchange(distribution=[broadcast])\
   +- [#3] TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])\
])
            :        :- Exchange(distribution=[broadcast])
            :        :  +- MultipleInput(readOrder=[0,1], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(ib_income_band_sk = w_warehouse_sk)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])\
])
            :        :     :- Exchange(distribution=[broadcast])
            :        :     :  +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
            :        :     +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
            :        :- Exchange(distribution=[broadcast])
            :        :  +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
            :        +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
            +- Exchange(distribution=[hash[ss_net_paid_node_11]])
               +- Calc(select=[ss_sold_date_sk AS jOU12, ss_sold_time_sk AS ss_sold_time_sk_node_11, ss_item_sk AS ss_item_sk_node_11, ss_customer_sk AS ss_customer_sk_node_11, ss_cdemo_sk AS ss_cdemo_sk_node_11, ss_hdemo_sk AS ss_hdemo_sk_node_11, ss_addr_sk AS ss_addr_sk_node_11, ss_store_sk AS ss_store_sk_node_11, ss_promo_sk AS ss_promo_sk_node_11, ss_ticket_number AS ss_ticket_number_node_11, ss_quantity AS ss_quantity_node_11, ss_wholesale_cost AS ss_wholesale_cost_node_11, ss_list_price AS ss_list_price_node_11, ss_sales_price AS ss_sales_price_node_11, ss_ext_discount_amt AS ss_ext_discount_amt_node_11, ss_ext_sales_price AS ss_ext_sales_price_node_11, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_11, ss_ext_list_price AS ss_ext_list_price_node_11, ss_ext_tax AS ss_ext_tax_node_11, ss_coupon_amt AS ss_coupon_amt_node_11, ss_net_paid AS ss_net_paid_node_11, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_11, ss_net_profit AS ss_net_profit_node_11])
                  +- Sort(orderBy=[ss_coupon_amt ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o10094514.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#19601298:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[19](input=RelSubset#19601296,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[19]), rel#19601295:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[19](input=RelSubset#19601294,groupBy=ss_ext_discount_amt, ss_net_paid,select=ss_ext_discount_amt, ss_net_paid, Partial_MAX(ss_sales_price) AS max$0)]
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