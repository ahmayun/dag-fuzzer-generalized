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
    return values.median()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_12 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_17 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_17") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_14 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_15 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_16 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_8 = autonode_12.filter(col('c_first_name_node_12').char_length >= 5)
autonode_11 = autonode_17.filter(col('i_rec_end_date_node_17').char_length >= 5)
autonode_9 = autonode_13.join(autonode_14, col('inv_item_sk_node_13') == col('wp_customer_sk_node_14'))
autonode_10 = autonode_15.join(autonode_16, col('ib_upper_bound_node_15') == col('cr_returning_cdemo_sk_node_16'))
autonode_5 = autonode_8.add_columns(lit("hello"))
autonode_6 = autonode_9.order_by(col('wp_link_count_node_14'))
autonode_7 = autonode_10.join(autonode_11, col('i_current_price_node_17') == col('cr_net_loss_node_16'))
autonode_3 = autonode_5.join(autonode_6, col('c_birth_day_node_12') == col('wp_creation_date_sk_node_14'))
autonode_4 = autonode_7.order_by(col('cr_order_number_node_16'))
autonode_1 = autonode_3.group_by(col('wp_link_count_node_14')).select(col('wp_creation_date_sk_node_14').min.alias('wp_creation_date_sk_node_14'))
autonode_2 = autonode_4.select(col('cr_item_sk_node_16'))
sink = autonode_1.join(autonode_2, col('cr_item_sk_node_16') == col('wp_creation_date_sk_node_14'))
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
LogicalJoin(condition=[=($1, $0)], joinType=[inner])
:- LogicalProject(wp_creation_date_sk_node_14=[$1])
:  +- LogicalAggregate(group=[{34}], EXPR$0=[MIN($27)])
:     +- LogicalJoin(condition=[=($11, $27)], joinType=[inner])
:        :- LogicalProject(c_customer_sk_node_12=[$0], c_customer_id_node_12=[$1], c_current_cdemo_sk_node_12=[$2], c_current_hdemo_sk_node_12=[$3], c_current_addr_sk_node_12=[$4], c_first_shipto_date_sk_node_12=[$5], c_first_sales_date_sk_node_12=[$6], c_salutation_node_12=[$7], c_first_name_node_12=[$8], c_last_name_node_12=[$9], c_preferred_cust_flag_node_12=[$10], c_birth_day_node_12=[$11], c_birth_month_node_12=[$12], c_birth_year_node_12=[$13], c_birth_country_node_12=[$14], c_login_node_12=[$15], c_email_address_node_12=[$16], c_last_review_date_node_12=[$17], _c18=[_UTF-16LE'hello'])
:        :  +- LogicalFilter(condition=[>=(CHAR_LENGTH($8), 5)])
:        :     +- LogicalProject(c_customer_sk_node_12=[$0], c_customer_id_node_12=[$1], c_current_cdemo_sk_node_12=[$2], c_current_hdemo_sk_node_12=[$3], c_current_addr_sk_node_12=[$4], c_first_shipto_date_sk_node_12=[$5], c_first_sales_date_sk_node_12=[$6], c_salutation_node_12=[$7], c_first_name_node_12=[$8], c_last_name_node_12=[$9], c_preferred_cust_flag_node_12=[$10], c_birth_day_node_12=[$11], c_birth_month_node_12=[$12], c_birth_year_node_12=[$13], c_birth_country_node_12=[$14], c_login_node_12=[$15], c_email_address_node_12=[$16], c_last_review_date_node_12=[$17])
:        :        +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
:        +- LogicalSort(sort0=[$15], dir0=[ASC])
:           +- LogicalJoin(condition=[=($1, $11)], joinType=[inner])
:              :- LogicalProject(inv_date_sk_node_13=[$0], inv_item_sk_node_13=[$1], inv_warehouse_sk_node_13=[$2], inv_quantity_on_hand_node_13=[$3])
:              :  +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])
:              +- LogicalProject(wp_web_page_sk_node_14=[$0], wp_web_page_id_node_14=[$1], wp_rec_start_date_node_14=[$2], wp_rec_end_date_node_14=[$3], wp_creation_date_sk_node_14=[$4], wp_access_date_sk_node_14=[$5], wp_autogen_flag_node_14=[$6], wp_customer_sk_node_14=[$7], wp_url_node_14=[$8], wp_type_node_14=[$9], wp_char_count_node_14=[$10], wp_link_count_node_14=[$11], wp_image_count_node_14=[$12], wp_max_ad_count_node_14=[$13])
:                 +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
+- LogicalProject(cr_item_sk_node_16=[$5])
   +- LogicalSort(sort0=[$19], dir0=[ASC])
      +- LogicalJoin(condition=[=($35, $29)], joinType=[inner])
         :- LogicalJoin(condition=[=($2, $11)], joinType=[inner])
         :  :- LogicalProject(ib_income_band_sk_node_15=[$0], ib_lower_bound_node_15=[$1], ib_upper_bound_node_15=[$2])
         :  :  +- LogicalTableScan(table=[[default_catalog, default_database, income_band]])
         :  +- LogicalProject(cr_returned_date_sk_node_16=[$0], cr_returned_time_sk_node_16=[$1], cr_item_sk_node_16=[$2], cr_refunded_customer_sk_node_16=[$3], cr_refunded_cdemo_sk_node_16=[$4], cr_refunded_hdemo_sk_node_16=[$5], cr_refunded_addr_sk_node_16=[$6], cr_returning_customer_sk_node_16=[$7], cr_returning_cdemo_sk_node_16=[$8], cr_returning_hdemo_sk_node_16=[$9], cr_returning_addr_sk_node_16=[$10], cr_call_center_sk_node_16=[$11], cr_catalog_page_sk_node_16=[$12], cr_ship_mode_sk_node_16=[$13], cr_warehouse_sk_node_16=[$14], cr_reason_sk_node_16=[$15], cr_order_number_node_16=[$16], cr_return_quantity_node_16=[$17], cr_return_amount_node_16=[$18], cr_return_tax_node_16=[$19], cr_return_amt_inc_tax_node_16=[$20], cr_fee_node_16=[$21], cr_return_ship_cost_node_16=[$22], cr_refunded_cash_node_16=[$23], cr_reversed_charge_node_16=[$24], cr_store_credit_node_16=[$25], cr_net_loss_node_16=[$26])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
         +- LogicalFilter(condition=[>=(CHAR_LENGTH($3), 5)])
            +- LogicalProject(i_item_sk_node_17=[$0], i_item_id_node_17=[$1], i_rec_start_date_node_17=[$2], i_rec_end_date_node_17=[$3], i_item_desc_node_17=[$4], i_current_price_node_17=[$5], i_wholesale_cost_node_17=[$6], i_brand_id_node_17=[$7], i_brand_node_17=[$8], i_class_id_node_17=[$9], i_class_node_17=[$10], i_category_id_node_17=[$11], i_category_node_17=[$12], i_manufact_id_node_17=[$13], i_manufact_node_17=[$14], i_size_node_17=[$15], i_formulation_node_17=[$16], i_color_node_17=[$17], i_units_node_17=[$18], i_container_node_17=[$19], i_manager_id_node_17=[$20], i_product_name_node_17=[$21])
               +- LogicalTableScan(table=[[default_catalog, default_database, item]])

== Optimized Physical Plan ==
HashJoin(joinType=[InnerJoin], where=[=(cr_item_sk_node_16, wp_creation_date_sk_node_14)], select=[wp_creation_date_sk_node_14, cr_item_sk_node_16], isBroadcast=[true], build=[right])
:- Calc(select=[EXPR$0 AS wp_creation_date_sk_node_14])
:  +- HashAggregate(isMerge=[false], groupBy=[wp_link_count], select=[wp_link_count, MIN(wp_creation_date_sk) AS EXPR$0])
:     +- Exchange(distribution=[hash[wp_link_count]])
:        +- NestedLoopJoin(joinType=[InnerJoin], where=[=(c_birth_day_node_12, wp_creation_date_sk)], select=[c_customer_sk_node_12, c_customer_id_node_12, c_current_cdemo_sk_node_12, c_current_hdemo_sk_node_12, c_current_addr_sk_node_12, c_first_shipto_date_sk_node_12, c_first_sales_date_sk_node_12, c_salutation_node_12, c_first_name_node_12, c_last_name_node_12, c_preferred_cust_flag_node_12, c_birth_day_node_12, c_birth_month_node_12, c_birth_year_node_12, c_birth_country_node_12, c_login_node_12, c_email_address_node_12, c_last_review_date_node_12, _c18, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], build=[left])
:           :- Exchange(distribution=[broadcast])
:           :  +- Calc(select=[c_customer_sk AS c_customer_sk_node_12, c_customer_id AS c_customer_id_node_12, c_current_cdemo_sk AS c_current_cdemo_sk_node_12, c_current_hdemo_sk AS c_current_hdemo_sk_node_12, c_current_addr_sk AS c_current_addr_sk_node_12, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_12, c_first_sales_date_sk AS c_first_sales_date_sk_node_12, c_salutation AS c_salutation_node_12, c_first_name AS c_first_name_node_12, c_last_name AS c_last_name_node_12, c_preferred_cust_flag AS c_preferred_cust_flag_node_12, c_birth_day AS c_birth_day_node_12, c_birth_month AS c_birth_month_node_12, c_birth_year AS c_birth_year_node_12, c_birth_country AS c_birth_country_node_12, c_login AS c_login_node_12, c_email_address AS c_email_address_node_12, c_last_review_date AS c_last_review_date_node_12, 'hello' AS _c18], where=[>=(CHAR_LENGTH(c_first_name), 5)])
:           :     +- TableSourceScan(table=[[default_catalog, default_database, customer, filter=[>=(CHAR_LENGTH(c_first_name), 5)]]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
:           +- Sort(orderBy=[wp_link_count ASC])
:              +- Exchange(distribution=[single])
:                 +- HashJoin(joinType=[InnerJoin], where=[=(inv_item_sk, wp_customer_sk)], select=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], isBroadcast=[true], build=[right])
:                    :- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
:                    +- Exchange(distribution=[broadcast])
:                       +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
+- Exchange(distribution=[broadcast])
   +- Calc(select=[cr_item_sk AS cr_item_sk_node_16])
      +- Sort(orderBy=[cr_order_number ASC])
         +- Exchange(distribution=[single])
            +- HashJoin(joinType=[InnerJoin], where=[=(i_current_price, cr_net_loss)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], build=[right])
               :- Exchange(distribution=[hash[cr_net_loss]])
               :  +- HashJoin(joinType=[InnerJoin], where=[=(ib_upper_bound, cr_returning_cdemo_sk)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], isBroadcast=[true], build=[left])
               :     :- Exchange(distribution=[broadcast])
               :     :  +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
               :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
               +- Exchange(distribution=[hash[i_current_price]])
                  +- Calc(select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], where=[>=(CHAR_LENGTH(i_rec_end_date), 5)])
                     +- TableSourceScan(table=[[default_catalog, default_database, item, filter=[>=(CHAR_LENGTH(i_rec_end_date), 5)]]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

== Optimized Execution Plan ==
MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(cr_item_sk_node_16 = wp_creation_date_sk_node_14)], select=[wp_creation_date_sk_node_14, cr_item_sk_node_16], isBroadcast=[true], build=[right])\
:- Calc(select=[EXPR$0 AS wp_creation_date_sk_node_14])\
:  +- HashAggregate(isMerge=[false], groupBy=[wp_link_count], select=[wp_link_count, MIN(wp_creation_date_sk) AS EXPR$0])\
:     +- [#2] Exchange(distribution=[hash[wp_link_count]])\
+- [#1] Exchange(distribution=[broadcast])\
])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[cr_item_sk AS cr_item_sk_node_16])
:     +- Sort(orderBy=[cr_order_number ASC])
:        +- Exchange(distribution=[single])
:           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(i_current_price = cr_net_loss)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], build=[right])
:              :- Exchange(distribution=[hash[cr_net_loss]])
:              :  +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(ib_upper_bound = cr_returning_cdemo_sk)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])\
])
:              :     :- Exchange(distribution=[broadcast])
:              :     :  +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
:              :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
:              +- Exchange(distribution=[hash[i_current_price]])
:                 +- Calc(select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], where=[(CHAR_LENGTH(i_rec_end_date) >= 5)])
:                    +- TableSourceScan(table=[[default_catalog, default_database, item, filter=[>=(CHAR_LENGTH(i_rec_end_date), 5)]]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
+- Exchange(distribution=[hash[wp_link_count]])
   +- NestedLoopJoin(joinType=[InnerJoin], where=[(c_birth_day_node_12 = wp_creation_date_sk)], select=[c_customer_sk_node_12, c_customer_id_node_12, c_current_cdemo_sk_node_12, c_current_hdemo_sk_node_12, c_current_addr_sk_node_12, c_first_shipto_date_sk_node_12, c_first_sales_date_sk_node_12, c_salutation_node_12, c_first_name_node_12, c_last_name_node_12, c_preferred_cust_flag_node_12, c_birth_day_node_12, c_birth_month_node_12, c_birth_year_node_12, c_birth_country_node_12, c_login_node_12, c_email_address_node_12, c_last_review_date_node_12, _c18, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], build=[left])
      :- Exchange(distribution=[broadcast])
      :  +- Calc(select=[c_customer_sk AS c_customer_sk_node_12, c_customer_id AS c_customer_id_node_12, c_current_cdemo_sk AS c_current_cdemo_sk_node_12, c_current_hdemo_sk AS c_current_hdemo_sk_node_12, c_current_addr_sk AS c_current_addr_sk_node_12, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_12, c_first_sales_date_sk AS c_first_sales_date_sk_node_12, c_salutation AS c_salutation_node_12, c_first_name AS c_first_name_node_12, c_last_name AS c_last_name_node_12, c_preferred_cust_flag AS c_preferred_cust_flag_node_12, c_birth_day AS c_birth_day_node_12, c_birth_month AS c_birth_month_node_12, c_birth_year AS c_birth_year_node_12, c_birth_country AS c_birth_country_node_12, c_login AS c_login_node_12, c_email_address AS c_email_address_node_12, c_last_review_date AS c_last_review_date_node_12, 'hello' AS _c18], where=[(CHAR_LENGTH(c_first_name) >= 5)])
      :     +- TableSourceScan(table=[[default_catalog, default_database, customer, filter=[>=(CHAR_LENGTH(c_first_name), 5)]]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
      +- Sort(orderBy=[wp_link_count ASC])
         +- Exchange(distribution=[single])
            +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(inv_item_sk = wp_customer_sk)], select=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])\
+- [#2] Exchange(distribution=[broadcast])\
])
               :- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
               +- Exchange(distribution=[broadcast])
                  +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o233554295.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#472219744:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[15](input=RelSubset#472219742,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[15]), rel#472219741:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[15](input=RelSubset#472219740,groupBy=wp_creation_date_sk, wp_link_count,select=wp_creation_date_sk, wp_link_count, Partial_MIN(wp_creation_date_sk) AS min$0)]
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