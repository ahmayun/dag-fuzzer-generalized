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
    return values.std()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_12 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_17 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_17") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_14 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_15 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_16 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_8 = autonode_12.join(autonode_13, col('wp_web_page_sk_node_12') == col('wr_refunded_customer_sk_node_13'))
autonode_11 = autonode_17.alias('BqrZC')
autonode_9 = autonode_14.add_columns(lit("hello"))
autonode_10 = autonode_15.join(autonode_16, col('ca_address_sk_node_16') == col('i_item_sk_node_15'))
autonode_5 = autonode_8.order_by(col('wr_reversed_charge_node_13'))
autonode_6 = autonode_9.filter(col('i_container_node_14').char_length < 5)
autonode_7 = autonode_10.join(autonode_11, col('wr_web_page_sk_node_17') == col('i_item_sk_node_15'))
autonode_3 = autonode_5.join(autonode_6, col('wr_fee_node_13') == col('i_current_price_node_14'))
autonode_4 = autonode_7.filter(col('ca_location_type_node_16').char_length <= 5)
autonode_2 = autonode_3.join(autonode_4, col('wp_rec_end_date_node_12') == col('i_color_node_15'))
autonode_1 = autonode_2.group_by(col('wr_net_loss_node_13')).select(col('wr_returning_addr_sk_node_13').max.alias('wr_returning_addr_sk_node_13'))
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
LogicalProject(wr_returning_addr_sk_node_13=[$1], _c1=[_UTF-16LE'hello'])
+- LogicalAggregate(group=[{37}], EXPR$0=[MAX($24)])
   +- LogicalJoin(condition=[=($3, $78)], joinType=[inner])
      :- LogicalJoin(condition=[=($32, $43)], joinType=[inner])
      :  :- LogicalSort(sort0=[$35], dir0=[ASC])
      :  :  +- LogicalJoin(condition=[=($0, $17)], joinType=[inner])
      :  :     :- LogicalProject(wp_web_page_sk_node_12=[$0], wp_web_page_id_node_12=[$1], wp_rec_start_date_node_12=[$2], wp_rec_end_date_node_12=[$3], wp_creation_date_sk_node_12=[$4], wp_access_date_sk_node_12=[$5], wp_autogen_flag_node_12=[$6], wp_customer_sk_node_12=[$7], wp_url_node_12=[$8], wp_type_node_12=[$9], wp_char_count_node_12=[$10], wp_link_count_node_12=[$11], wp_image_count_node_12=[$12], wp_max_ad_count_node_12=[$13])
      :  :     :  +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
      :  :     +- LogicalProject(wr_returned_date_sk_node_13=[$0], wr_returned_time_sk_node_13=[$1], wr_item_sk_node_13=[$2], wr_refunded_customer_sk_node_13=[$3], wr_refunded_cdemo_sk_node_13=[$4], wr_refunded_hdemo_sk_node_13=[$5], wr_refunded_addr_sk_node_13=[$6], wr_returning_customer_sk_node_13=[$7], wr_returning_cdemo_sk_node_13=[$8], wr_returning_hdemo_sk_node_13=[$9], wr_returning_addr_sk_node_13=[$10], wr_web_page_sk_node_13=[$11], wr_reason_sk_node_13=[$12], wr_order_number_node_13=[$13], wr_return_quantity_node_13=[$14], wr_return_amt_node_13=[$15], wr_return_tax_node_13=[$16], wr_return_amt_inc_tax_node_13=[$17], wr_fee_node_13=[$18], wr_return_ship_cost_node_13=[$19], wr_refunded_cash_node_13=[$20], wr_reversed_charge_node_13=[$21], wr_account_credit_node_13=[$22], wr_net_loss_node_13=[$23])
      :  :        +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
      :  +- LogicalFilter(condition=[<(CHAR_LENGTH($19), 5)])
      :     +- LogicalProject(i_item_sk_node_14=[$0], i_item_id_node_14=[$1], i_rec_start_date_node_14=[$2], i_rec_end_date_node_14=[$3], i_item_desc_node_14=[$4], i_current_price_node_14=[$5], i_wholesale_cost_node_14=[$6], i_brand_id_node_14=[$7], i_brand_node_14=[$8], i_class_id_node_14=[$9], i_class_node_14=[$10], i_category_id_node_14=[$11], i_category_node_14=[$12], i_manufact_id_node_14=[$13], i_manufact_node_14=[$14], i_size_node_14=[$15], i_formulation_node_14=[$16], i_color_node_14=[$17], i_units_node_14=[$18], i_container_node_14=[$19], i_manager_id_node_14=[$20], i_product_name_node_14=[$21], _c22=[_UTF-16LE'hello'])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, item]])
      +- LogicalFilter(condition=[<=(CHAR_LENGTH($34), 5)])
         +- LogicalJoin(condition=[=($46, $0)], joinType=[inner])
            :- LogicalJoin(condition=[=($22, $0)], joinType=[inner])
            :  :- LogicalProject(i_item_sk_node_15=[$0], i_item_id_node_15=[$1], i_rec_start_date_node_15=[$2], i_rec_end_date_node_15=[$3], i_item_desc_node_15=[$4], i_current_price_node_15=[$5], i_wholesale_cost_node_15=[$6], i_brand_id_node_15=[$7], i_brand_node_15=[$8], i_class_id_node_15=[$9], i_class_node_15=[$10], i_category_id_node_15=[$11], i_category_node_15=[$12], i_manufact_id_node_15=[$13], i_manufact_node_15=[$14], i_size_node_15=[$15], i_formulation_node_15=[$16], i_color_node_15=[$17], i_units_node_15=[$18], i_container_node_15=[$19], i_manager_id_node_15=[$20], i_product_name_node_15=[$21])
            :  :  +- LogicalTableScan(table=[[default_catalog, default_database, item]])
            :  +- LogicalProject(ca_address_sk_node_16=[$0], ca_address_id_node_16=[$1], ca_street_number_node_16=[$2], ca_street_name_node_16=[$3], ca_street_type_node_16=[$4], ca_suite_number_node_16=[$5], ca_city_node_16=[$6], ca_county_node_16=[$7], ca_state_node_16=[$8], ca_zip_node_16=[$9], ca_country_node_16=[$10], ca_gmt_offset_node_16=[$11], ca_location_type_node_16=[$12])
            :     +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
            +- LogicalProject(BqrZC=[AS($0, _UTF-16LE'BqrZC')], wr_returned_time_sk_node_17=[$1], wr_item_sk_node_17=[$2], wr_refunded_customer_sk_node_17=[$3], wr_refunded_cdemo_sk_node_17=[$4], wr_refunded_hdemo_sk_node_17=[$5], wr_refunded_addr_sk_node_17=[$6], wr_returning_customer_sk_node_17=[$7], wr_returning_cdemo_sk_node_17=[$8], wr_returning_hdemo_sk_node_17=[$9], wr_returning_addr_sk_node_17=[$10], wr_web_page_sk_node_17=[$11], wr_reason_sk_node_17=[$12], wr_order_number_node_17=[$13], wr_return_quantity_node_17=[$14], wr_return_amt_node_17=[$15], wr_return_tax_node_17=[$16], wr_return_amt_inc_tax_node_17=[$17], wr_fee_node_17=[$18], wr_return_ship_cost_node_17=[$19], wr_refunded_cash_node_17=[$20], wr_reversed_charge_node_17=[$21], wr_account_credit_node_17=[$22], wr_net_loss_node_17=[$23])
               +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS wr_returning_addr_sk_node_13, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[wr_net_loss], select=[wr_net_loss, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[wr_net_loss]])
      +- LocalHashAggregate(groupBy=[wr_net_loss], select=[wr_net_loss, Partial_MAX(EXPR$0) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(wp_rec_end_date, i_color)], select=[wp_rec_end_date, wr_net_loss, EXPR$0, i_color], isBroadcast=[true], build=[right])
            :- HashAggregate(isMerge=[false], groupBy=[wp_rec_end_date, wr_net_loss], select=[wp_rec_end_date, wr_net_loss, MAX(wr_returning_addr_sk) AS EXPR$0])
            :  +- Exchange(distribution=[hash[wp_rec_end_date, wr_net_loss]])
            :     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(wr_fee, i_current_price_node_14)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, i_item_sk_node_14, i_item_id_node_14, i_rec_start_date_node_14, i_rec_end_date_node_14, i_item_desc_node_14, i_current_price_node_14, i_wholesale_cost_node_14, i_brand_id_node_14, i_brand_node_14, i_class_id_node_14, i_class_node_14, i_category_id_node_14, i_category_node_14, i_manufact_id_node_14, i_manufact_node_14, i_size_node_14, i_formulation_node_14, i_color_node_14, i_units_node_14, i_container_node_14, i_manager_id_node_14, i_product_name_node_14, _c22], build=[right])
            :        :- Sort(orderBy=[wr_reversed_charge ASC])
            :        :  +- Exchange(distribution=[single])
            :        :     +- HashJoin(joinType=[InnerJoin], where=[=(wp_web_page_sk, wr_refunded_customer_sk)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
            :        :        :- Exchange(distribution=[broadcast])
            :        :        :  +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
            :        :        +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
            :        +- Exchange(distribution=[broadcast])
            :           +- Calc(select=[i_item_sk AS i_item_sk_node_14, i_item_id AS i_item_id_node_14, i_rec_start_date AS i_rec_start_date_node_14, i_rec_end_date AS i_rec_end_date_node_14, i_item_desc AS i_item_desc_node_14, i_current_price AS i_current_price_node_14, i_wholesale_cost AS i_wholesale_cost_node_14, i_brand_id AS i_brand_id_node_14, i_brand AS i_brand_node_14, i_class_id AS i_class_id_node_14, i_class AS i_class_node_14, i_category_id AS i_category_id_node_14, i_category AS i_category_node_14, i_manufact_id AS i_manufact_id_node_14, i_manufact AS i_manufact_node_14, i_size AS i_size_node_14, i_formulation AS i_formulation_node_14, i_color AS i_color_node_14, i_units AS i_units_node_14, i_container AS i_container_node_14, i_manager_id AS i_manager_id_node_14, i_product_name AS i_product_name_node_14, 'hello' AS _c22], where=[<(CHAR_LENGTH(i_container), 5)])
            :              +- TableSourceScan(table=[[default_catalog, default_database, item, filter=[<(CHAR_LENGTH(i_container), 5)]]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
            +- Exchange(distribution=[broadcast])
               +- HashAggregate(isMerge=[true], groupBy=[i_color], select=[i_color])
                  +- Exchange(distribution=[hash[i_color]])
                     +- LocalHashAggregate(groupBy=[i_color], select=[i_color])
                        +- HashJoin(joinType=[InnerJoin], where=[=(wr_web_page_sk, i_item_sk)], select=[i_item_sk, i_color, wr_web_page_sk], isBroadcast=[true], build=[left])
                           :- Exchange(distribution=[broadcast])
                           :  +- HashAggregate(isMerge=[false], groupBy=[i_item_sk, i_color], select=[i_item_sk, i_color])
                           :     +- HashJoin(joinType=[InnerJoin], where=[=(ca_address_sk, i_item_sk)], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], build=[right])
                           :        :- Exchange(distribution=[hash[i_item_sk]])
                           :        :  +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
                           :        +- Exchange(distribution=[hash[ca_address_sk]])
                           :           +- Calc(select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], where=[<=(CHAR_LENGTH(ca_location_type), 5)])
                           :              +- TableSourceScan(table=[[default_catalog, default_database, customer_address, filter=[<=(CHAR_LENGTH(ca_location_type), 5)]]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                           +- HashAggregate(isMerge=[true], groupBy=[wr_web_page_sk], select=[wr_web_page_sk])
                              +- Exchange(distribution=[hash[wr_web_page_sk]])
                                 +- LocalHashAggregate(groupBy=[wr_web_page_sk], select=[wr_web_page_sk])
                                    +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS wr_returning_addr_sk_node_13, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[wr_net_loss], select=[wr_net_loss, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[wr_net_loss]])
      +- LocalHashAggregate(groupBy=[wr_net_loss], select=[wr_net_loss, Partial_MAX(EXPR$0) AS max$0])
         +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(wp_rec_end_date = i_color)], select=[wp_rec_end_date, wr_net_loss, EXPR$0, i_color], isBroadcast=[true], build=[right])\
:- HashAggregate(isMerge=[false], groupBy=[wp_rec_end_date, wr_net_loss], select=[wp_rec_end_date, wr_net_loss, MAX(wr_returning_addr_sk) AS EXPR$0])\
:  +- [#2] Exchange(distribution=[hash[wp_rec_end_date, wr_net_loss]])\
+- [#1] Exchange(distribution=[broadcast])\
])
            :- Exchange(distribution=[broadcast])
            :  +- HashAggregate(isMerge=[true], groupBy=[i_color], select=[i_color])
            :     +- Exchange(distribution=[hash[i_color]])
            :        +- LocalHashAggregate(groupBy=[i_color], select=[i_color])
            :           +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(wr_web_page_sk = i_item_sk)], select=[i_item_sk, i_color, wr_web_page_sk], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- HashAggregate(isMerge=[true], groupBy=[wr_web_page_sk], select=[wr_web_page_sk])\
   +- [#2] Exchange(distribution=[hash[wr_web_page_sk]])\
])
            :              :- Exchange(distribution=[broadcast])
            :              :  +- HashAggregate(isMerge=[false], groupBy=[i_item_sk, i_color], select=[i_item_sk, i_color])
            :              :     +- Exchange(distribution=[keep_input_as_is[hash[i_item_sk, i_color]]])
            :              :        +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ca_address_sk = i_item_sk)], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], build=[right])
            :              :           :- Exchange(distribution=[hash[i_item_sk]])
            :              :           :  +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
            :              :           +- Exchange(distribution=[hash[ca_address_sk]])
            :              :              +- Calc(select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], where=[(CHAR_LENGTH(ca_location_type) <= 5)])
            :              :                 +- TableSourceScan(table=[[default_catalog, default_database, customer_address, filter=[<=(CHAR_LENGTH(ca_location_type), 5)]]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
            :              +- Exchange(distribution=[hash[wr_web_page_sk]])
            :                 +- LocalHashAggregate(groupBy=[wr_web_page_sk], select=[wr_web_page_sk])
            :                    +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])(reuse_id=[1])
            +- Exchange(distribution=[hash[wp_rec_end_date, wr_net_loss]])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[(wr_fee = i_current_price_node_14)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, i_item_sk_node_14, i_item_id_node_14, i_rec_start_date_node_14, i_rec_end_date_node_14, i_item_desc_node_14, i_current_price_node_14, i_wholesale_cost_node_14, i_brand_id_node_14, i_brand_node_14, i_class_id_node_14, i_class_node_14, i_category_id_node_14, i_category_node_14, i_manufact_id_node_14, i_manufact_node_14, i_size_node_14, i_formulation_node_14, i_color_node_14, i_units_node_14, i_container_node_14, i_manager_id_node_14, i_product_name_node_14, _c22], build=[right])
                  :- Sort(orderBy=[wr_reversed_charge ASC])
                  :  +- Exchange(distribution=[single])
                  :     +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(wp_web_page_sk = wr_refunded_customer_sk)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])\
])
                  :        :- Exchange(distribution=[broadcast])
                  :        :  +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                  :        +- Reused(reference_id=[1])
                  +- Exchange(distribution=[broadcast])
                     +- Calc(select=[i_item_sk AS i_item_sk_node_14, i_item_id AS i_item_id_node_14, i_rec_start_date AS i_rec_start_date_node_14, i_rec_end_date AS i_rec_end_date_node_14, i_item_desc AS i_item_desc_node_14, i_current_price AS i_current_price_node_14, i_wholesale_cost AS i_wholesale_cost_node_14, i_brand_id AS i_brand_id_node_14, i_brand AS i_brand_node_14, i_class_id AS i_class_id_node_14, i_class AS i_class_node_14, i_category_id AS i_category_id_node_14, i_category AS i_category_node_14, i_manufact_id AS i_manufact_id_node_14, i_manufact AS i_manufact_node_14, i_size AS i_size_node_14, i_formulation AS i_formulation_node_14, i_color AS i_color_node_14, i_units AS i_units_node_14, i_container AS i_container_node_14, i_manager_id AS i_manager_id_node_14, i_product_name AS i_product_name_node_14, 'hello' AS _c22], where=[(CHAR_LENGTH(i_container) < 5)])
                        +- TableSourceScan(table=[[default_catalog, default_database, item, filter=[<(CHAR_LENGTH(i_container), 5)]]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o127744554.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#257705800:AbstractConverter.BATCH_PHYSICAL.hash[0, 1, 2]true.[35](input=RelSubset#257705798,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1, 2]true,sort=[35]), rel#257705797:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[35](input=RelSubset#257705796,groupBy=wp_rec_end_date, wr_fee, wr_net_loss,select=wp_rec_end_date, wr_fee, wr_net_loss, Partial_MAX(wr_returning_addr_sk) AS max$0)]
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