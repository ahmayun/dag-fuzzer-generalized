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
    import numpy as np
    return np.exp(np.log(values[values > 0]).mean()) if (values > 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_9 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_11 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_7 = autonode_9.order_by(col('cp_description_node_9'))
autonode_8 = autonode_10.join(autonode_11, col('ca_address_sk_node_11') == col('ws_sold_time_sk_node_10'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_6 = autonode_8.alias('HQ1pq')
autonode_3 = autonode_5.filter(col('cp_end_date_sk_node_9') <= 29)
autonode_4 = autonode_6.alias('eFGGN')
autonode_2 = autonode_3.join(autonode_4, col('cp_catalog_page_sk_node_9') == col('ca_address_sk_node_11'))
autonode_1 = autonode_2.group_by(col('cp_catalog_number_node_9')).select(col('ca_street_number_node_11').count.alias('ca_street_number_node_11'))
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
LogicalProject(ca_street_number_node_11=[$1], _c1=[_UTF-16LE'hello'])
+- LogicalAggregate(group=[{5}], EXPR$0=[COUNT($46)])
   +- LogicalJoin(condition=[=($0, $44)], joinType=[inner])
      :- LogicalFilter(condition=[<=($3, 29)])
      :  +- LogicalProject(cp_catalog_page_sk_node_9=[$0], cp_catalog_page_id_node_9=[$1], cp_start_date_sk_node_9=[$2], cp_end_date_sk_node_9=[$3], cp_department_node_9=[$4], cp_catalog_number_node_9=[$5], cp_catalog_page_number_node_9=[$6], cp_description_node_9=[$7], cp_type_node_9=[$8], _c9=[_UTF-16LE'hello'])
      :     +- LogicalSort(sort0=[$7], dir0=[ASC])
      :        +- LogicalProject(cp_catalog_page_sk_node_9=[$0], cp_catalog_page_id_node_9=[$1], cp_start_date_sk_node_9=[$2], cp_end_date_sk_node_9=[$3], cp_department_node_9=[$4], cp_catalog_number_node_9=[$5], cp_catalog_page_number_node_9=[$6], cp_description_node_9=[$7], cp_type_node_9=[$8])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])
      +- LogicalProject(eFGGN=[AS(AS($0, _UTF-16LE'HQ1pq'), _UTF-16LE'eFGGN')], ws_sold_time_sk_node_10=[$1], ws_ship_date_sk_node_10=[$2], ws_item_sk_node_10=[$3], ws_bill_customer_sk_node_10=[$4], ws_bill_cdemo_sk_node_10=[$5], ws_bill_hdemo_sk_node_10=[$6], ws_bill_addr_sk_node_10=[$7], ws_ship_customer_sk_node_10=[$8], ws_ship_cdemo_sk_node_10=[$9], ws_ship_hdemo_sk_node_10=[$10], ws_ship_addr_sk_node_10=[$11], ws_web_page_sk_node_10=[$12], ws_web_site_sk_node_10=[$13], ws_ship_mode_sk_node_10=[$14], ws_warehouse_sk_node_10=[$15], ws_promo_sk_node_10=[$16], ws_order_number_node_10=[$17], ws_quantity_node_10=[$18], ws_wholesale_cost_node_10=[$19], ws_list_price_node_10=[$20], ws_sales_price_node_10=[$21], ws_ext_discount_amt_node_10=[$22], ws_ext_sales_price_node_10=[$23], ws_ext_wholesale_cost_node_10=[$24], ws_ext_list_price_node_10=[$25], ws_ext_tax_node_10=[$26], ws_coupon_amt_node_10=[$27], ws_ext_ship_cost_node_10=[$28], ws_net_paid_node_10=[$29], ws_net_paid_inc_tax_node_10=[$30], ws_net_paid_inc_ship_node_10=[$31], ws_net_paid_inc_ship_tax_node_10=[$32], ws_net_profit_node_10=[$33], ca_address_sk_node_11=[$34], ca_address_id_node_11=[$35], ca_street_number_node_11=[$36], ca_street_name_node_11=[$37], ca_street_type_node_11=[$38], ca_suite_number_node_11=[$39], ca_city_node_11=[$40], ca_county_node_11=[$41], ca_state_node_11=[$42], ca_zip_node_11=[$43], ca_country_node_11=[$44], ca_gmt_offset_node_11=[$45], ca_location_type_node_11=[$46])
         +- LogicalJoin(condition=[=($34, $1)], joinType=[inner])
            :- LogicalProject(ws_sold_date_sk_node_10=[$0], ws_sold_time_sk_node_10=[$1], ws_ship_date_sk_node_10=[$2], ws_item_sk_node_10=[$3], ws_bill_customer_sk_node_10=[$4], ws_bill_cdemo_sk_node_10=[$5], ws_bill_hdemo_sk_node_10=[$6], ws_bill_addr_sk_node_10=[$7], ws_ship_customer_sk_node_10=[$8], ws_ship_cdemo_sk_node_10=[$9], ws_ship_hdemo_sk_node_10=[$10], ws_ship_addr_sk_node_10=[$11], ws_web_page_sk_node_10=[$12], ws_web_site_sk_node_10=[$13], ws_ship_mode_sk_node_10=[$14], ws_warehouse_sk_node_10=[$15], ws_promo_sk_node_10=[$16], ws_order_number_node_10=[$17], ws_quantity_node_10=[$18], ws_wholesale_cost_node_10=[$19], ws_list_price_node_10=[$20], ws_sales_price_node_10=[$21], ws_ext_discount_amt_node_10=[$22], ws_ext_sales_price_node_10=[$23], ws_ext_wholesale_cost_node_10=[$24], ws_ext_list_price_node_10=[$25], ws_ext_tax_node_10=[$26], ws_coupon_amt_node_10=[$27], ws_ext_ship_cost_node_10=[$28], ws_net_paid_node_10=[$29], ws_net_paid_inc_tax_node_10=[$30], ws_net_paid_inc_ship_node_10=[$31], ws_net_paid_inc_ship_tax_node_10=[$32], ws_net_profit_node_10=[$33])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
            +- LogicalProject(ca_address_sk_node_11=[$0], ca_address_id_node_11=[$1], ca_street_number_node_11=[$2], ca_street_name_node_11=[$3], ca_street_type_node_11=[$4], ca_suite_number_node_11=[$5], ca_city_node_11=[$6], ca_county_node_11=[$7], ca_state_node_11=[$8], ca_zip_node_11=[$9], ca_country_node_11=[$10], ca_gmt_offset_node_11=[$11], ca_location_type_node_11=[$12])
               +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ca_street_number_node_11, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[cp_catalog_number_node_9], select=[cp_catalog_number_node_9, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cp_catalog_number_node_9]])
      +- LocalHashAggregate(groupBy=[cp_catalog_number_node_9], select=[cp_catalog_number_node_9, Partial_COUNT(ca_street_number_node_11) AS count$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(cp_catalog_page_sk_node_9, ca_address_sk_node_11)], select=[cp_catalog_page_sk_node_9, cp_catalog_page_id_node_9, cp_start_date_sk_node_9, cp_end_date_sk_node_9, cp_department_node_9, cp_catalog_number_node_9, cp_catalog_page_number_node_9, cp_description_node_9, cp_type_node_9, _c9, eFGGN, ws_sold_time_sk_node_10, ws_ship_date_sk_node_10, ws_item_sk_node_10, ws_bill_customer_sk_node_10, ws_bill_cdemo_sk_node_10, ws_bill_hdemo_sk_node_10, ws_bill_addr_sk_node_10, ws_ship_customer_sk_node_10, ws_ship_cdemo_sk_node_10, ws_ship_hdemo_sk_node_10, ws_ship_addr_sk_node_10, ws_web_page_sk_node_10, ws_web_site_sk_node_10, ws_ship_mode_sk_node_10, ws_warehouse_sk_node_10, ws_promo_sk_node_10, ws_order_number_node_10, ws_quantity_node_10, ws_wholesale_cost_node_10, ws_list_price_node_10, ws_sales_price_node_10, ws_ext_discount_amt_node_10, ws_ext_sales_price_node_10, ws_ext_wholesale_cost_node_10, ws_ext_list_price_node_10, ws_ext_tax_node_10, ws_coupon_amt_node_10, ws_ext_ship_cost_node_10, ws_net_paid_node_10, ws_net_paid_inc_tax_node_10, ws_net_paid_inc_ship_node_10, ws_net_paid_inc_ship_tax_node_10, ws_net_profit_node_10, ca_address_sk_node_11, ca_address_id_node_11, ca_street_number_node_11, ca_street_name_node_11, ca_street_type_node_11, ca_suite_number_node_11, ca_city_node_11, ca_county_node_11, ca_state_node_11, ca_zip_node_11, ca_country_node_11, ca_gmt_offset_node_11, ca_location_type_node_11], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[cp_catalog_page_sk AS cp_catalog_page_sk_node_9, cp_catalog_page_id AS cp_catalog_page_id_node_9, cp_start_date_sk AS cp_start_date_sk_node_9, cp_end_date_sk AS cp_end_date_sk_node_9, cp_department AS cp_department_node_9, cp_catalog_number AS cp_catalog_number_node_9, cp_catalog_page_number AS cp_catalog_page_number_node_9, cp_description AS cp_description_node_9, cp_type AS cp_type_node_9, 'hello' AS _c9], where=[<=(cp_end_date_sk, 29)])
            :     +- Sort(orderBy=[cp_description ASC])
            :        +- Exchange(distribution=[single])
            :           +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
            +- Calc(select=[ws_sold_date_sk AS eFGGN, ws_sold_time_sk AS ws_sold_time_sk_node_10, ws_ship_date_sk AS ws_ship_date_sk_node_10, ws_item_sk AS ws_item_sk_node_10, ws_bill_customer_sk AS ws_bill_customer_sk_node_10, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_10, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_10, ws_bill_addr_sk AS ws_bill_addr_sk_node_10, ws_ship_customer_sk AS ws_ship_customer_sk_node_10, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_10, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_10, ws_ship_addr_sk AS ws_ship_addr_sk_node_10, ws_web_page_sk AS ws_web_page_sk_node_10, ws_web_site_sk AS ws_web_site_sk_node_10, ws_ship_mode_sk AS ws_ship_mode_sk_node_10, ws_warehouse_sk AS ws_warehouse_sk_node_10, ws_promo_sk AS ws_promo_sk_node_10, ws_order_number AS ws_order_number_node_10, ws_quantity AS ws_quantity_node_10, ws_wholesale_cost AS ws_wholesale_cost_node_10, ws_list_price AS ws_list_price_node_10, ws_sales_price AS ws_sales_price_node_10, ws_ext_discount_amt AS ws_ext_discount_amt_node_10, ws_ext_sales_price AS ws_ext_sales_price_node_10, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_10, ws_ext_list_price AS ws_ext_list_price_node_10, ws_ext_tax AS ws_ext_tax_node_10, ws_coupon_amt AS ws_coupon_amt_node_10, ws_ext_ship_cost AS ws_ext_ship_cost_node_10, ws_net_paid AS ws_net_paid_node_10, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_10, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_10, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_10, ws_net_profit AS ws_net_profit_node_10, ca_address_sk AS ca_address_sk_node_11, ca_address_id AS ca_address_id_node_11, ca_street_number AS ca_street_number_node_11, ca_street_name AS ca_street_name_node_11, ca_street_type AS ca_street_type_node_11, ca_suite_number AS ca_suite_number_node_11, ca_city AS ca_city_node_11, ca_county AS ca_county_node_11, ca_state AS ca_state_node_11, ca_zip AS ca_zip_node_11, ca_country AS ca_country_node_11, ca_gmt_offset AS ca_gmt_offset_node_11, ca_location_type AS ca_location_type_node_11])
               +- HashJoin(joinType=[InnerJoin], where=[=(ca_address_sk, ws_sold_time_sk)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], build=[right])
                  :- Exchange(distribution=[hash[ws_sold_time_sk]])
                  :  +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                  +- Exchange(distribution=[hash[ca_address_sk]])
                     +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ca_street_number_node_11, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[cp_catalog_number_node_9], select=[cp_catalog_number_node_9, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cp_catalog_number_node_9]])
      +- LocalHashAggregate(groupBy=[cp_catalog_number_node_9], select=[cp_catalog_number_node_9, Partial_COUNT(ca_street_number_node_11) AS count$0])
         +- MultipleInput(readOrder=[0,1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(cp_catalog_page_sk_node_9 = ca_address_sk_node_11)], select=[cp_catalog_page_sk_node_9, cp_catalog_page_id_node_9, cp_start_date_sk_node_9, cp_end_date_sk_node_9, cp_department_node_9, cp_catalog_number_node_9, cp_catalog_page_number_node_9, cp_description_node_9, cp_type_node_9, _c9, eFGGN, ws_sold_time_sk_node_10, ws_ship_date_sk_node_10, ws_item_sk_node_10, ws_bill_customer_sk_node_10, ws_bill_cdemo_sk_node_10, ws_bill_hdemo_sk_node_10, ws_bill_addr_sk_node_10, ws_ship_customer_sk_node_10, ws_ship_cdemo_sk_node_10, ws_ship_hdemo_sk_node_10, ws_ship_addr_sk_node_10, ws_web_page_sk_node_10, ws_web_site_sk_node_10, ws_ship_mode_sk_node_10, ws_warehouse_sk_node_10, ws_promo_sk_node_10, ws_order_number_node_10, ws_quantity_node_10, ws_wholesale_cost_node_10, ws_list_price_node_10, ws_sales_price_node_10, ws_ext_discount_amt_node_10, ws_ext_sales_price_node_10, ws_ext_wholesale_cost_node_10, ws_ext_list_price_node_10, ws_ext_tax_node_10, ws_coupon_amt_node_10, ws_ext_ship_cost_node_10, ws_net_paid_node_10, ws_net_paid_inc_tax_node_10, ws_net_paid_inc_ship_node_10, ws_net_paid_inc_ship_tax_node_10, ws_net_profit_node_10, ca_address_sk_node_11, ca_address_id_node_11, ca_street_number_node_11, ca_street_name_node_11, ca_street_type_node_11, ca_suite_number_node_11, ca_city_node_11, ca_county_node_11, ca_state_node_11, ca_zip_node_11, ca_country_node_11, ca_gmt_offset_node_11, ca_location_type_node_11], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- Calc(select=[ws_sold_date_sk AS eFGGN, ws_sold_time_sk AS ws_sold_time_sk_node_10, ws_ship_date_sk AS ws_ship_date_sk_node_10, ws_item_sk AS ws_item_sk_node_10, ws_bill_customer_sk AS ws_bill_customer_sk_node_10, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_10, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_10, ws_bill_addr_sk AS ws_bill_addr_sk_node_10, ws_ship_customer_sk AS ws_ship_customer_sk_node_10, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_10, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_10, ws_ship_addr_sk AS ws_ship_addr_sk_node_10, ws_web_page_sk AS ws_web_page_sk_node_10, ws_web_site_sk AS ws_web_site_sk_node_10, ws_ship_mode_sk AS ws_ship_mode_sk_node_10, ws_warehouse_sk AS ws_warehouse_sk_node_10, ws_promo_sk AS ws_promo_sk_node_10, ws_order_number AS ws_order_number_node_10, ws_quantity AS ws_quantity_node_10, ws_wholesale_cost AS ws_wholesale_cost_node_10, ws_list_price AS ws_list_price_node_10, ws_sales_price AS ws_sales_price_node_10, ws_ext_discount_amt AS ws_ext_discount_amt_node_10, ws_ext_sales_price AS ws_ext_sales_price_node_10, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_10, ws_ext_list_price AS ws_ext_list_price_node_10, ws_ext_tax AS ws_ext_tax_node_10, ws_coupon_amt AS ws_coupon_amt_node_10, ws_ext_ship_cost AS ws_ext_ship_cost_node_10, ws_net_paid AS ws_net_paid_node_10, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_10, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_10, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_10, ws_net_profit AS ws_net_profit_node_10, ca_address_sk AS ca_address_sk_node_11, ca_address_id AS ca_address_id_node_11, ca_street_number AS ca_street_number_node_11, ca_street_name AS ca_street_name_node_11, ca_street_type AS ca_street_type_node_11, ca_suite_number AS ca_suite_number_node_11, ca_city AS ca_city_node_11, ca_county AS ca_county_node_11, ca_state AS ca_state_node_11, ca_zip AS ca_zip_node_11, ca_country AS ca_country_node_11, ca_gmt_offset AS ca_gmt_offset_node_11, ca_location_type AS ca_location_type_node_11])\
   +- HashJoin(joinType=[InnerJoin], where=[(ca_address_sk = ws_sold_time_sk)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], build=[right])\
      :- [#2] Exchange(distribution=[hash[ws_sold_time_sk]])\
      +- [#3] Exchange(distribution=[hash[ca_address_sk]])\
])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[cp_catalog_page_sk AS cp_catalog_page_sk_node_9, cp_catalog_page_id AS cp_catalog_page_id_node_9, cp_start_date_sk AS cp_start_date_sk_node_9, cp_end_date_sk AS cp_end_date_sk_node_9, cp_department AS cp_department_node_9, cp_catalog_number AS cp_catalog_number_node_9, cp_catalog_page_number AS cp_catalog_page_number_node_9, cp_description AS cp_description_node_9, cp_type AS cp_type_node_9, 'hello' AS _c9], where=[(cp_end_date_sk <= 29)])
            :     +- Sort(orderBy=[cp_description ASC])
            :        +- Exchange(distribution=[single])
            :           +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
            :- Exchange(distribution=[hash[ws_sold_time_sk]])
            :  +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
            +- Exchange(distribution=[hash[ca_address_sk]])
               +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o37471418.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#74260425:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[7](input=RelSubset#74260423,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[7]), rel#74260422:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[7](input=RelSubset#74260421,groupBy=cp_catalog_page_sk, cp_catalog_number,select=cp_catalog_page_sk, cp_catalog_number, Partial_COUNT(*) AS count1$0)]
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