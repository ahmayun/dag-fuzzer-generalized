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
    return values.kurtosis()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_17 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_17") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_18 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_18") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_19 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_19") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_20 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_20") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_16 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_13 = autonode_17.join(autonode_18, col('ib_upper_bound_node_18') == col('s_market_id_node_17'))
autonode_14 = autonode_19.select(col('ca_street_number_node_19'))
autonode_15 = autonode_20.order_by(col('i_manufact_id_node_20'))
autonode_12 = autonode_16.filter(col('cs_call_center_sk_node_16') >= 34)
autonode_9 = autonode_13.order_by(col('s_country_node_17'))
autonode_10 = autonode_14.filter(col('ca_street_number_node_19').char_length > 5)
autonode_11 = autonode_15.order_by(col('i_category_node_20'))
autonode_8 = autonode_12.limit(75)
autonode_5 = autonode_9.limit(45)
autonode_6 = autonode_10.add_columns(lit("hello"))
autonode_7 = autonode_11.limit(33)
autonode_4 = autonode_8.order_by(col('cs_ext_discount_amt_node_16'))
autonode_3 = autonode_6.join(autonode_7, col('ca_street_number_node_19') == col('i_class_node_20'))
autonode_2 = autonode_4.join(autonode_5, col('cs_net_paid_inc_tax_node_16') == col('s_gmt_offset_node_17'))
autonode_1 = autonode_2.join(autonode_3, col('i_rec_end_date_node_20') == col('s_street_number_node_17'))
sink = autonode_1.group_by(col('i_current_price_node_20')).select(col('cs_ext_tax_node_16').max.alias('cs_ext_tax_node_16'))
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
LogicalProject(cs_ext_tax_node_16=[$1])
+- LogicalAggregate(group=[{73}], EXPR$0=[MAX($26)])
   +- LogicalJoin(condition=[=($71, $52)], joinType=[inner])
      :- LogicalJoin(condition=[=($30, $61)], joinType=[inner])
      :  :- LogicalSort(sort0=[$22], dir0=[ASC])
      :  :  +- LogicalSort(fetch=[75])
      :  :     +- LogicalFilter(condition=[>=($11, 34)])
      :  :        +- LogicalProject(cs_sold_date_sk_node_16=[$0], cs_sold_time_sk_node_16=[$1], cs_ship_date_sk_node_16=[$2], cs_bill_customer_sk_node_16=[$3], cs_bill_cdemo_sk_node_16=[$4], cs_bill_hdemo_sk_node_16=[$5], cs_bill_addr_sk_node_16=[$6], cs_ship_customer_sk_node_16=[$7], cs_ship_cdemo_sk_node_16=[$8], cs_ship_hdemo_sk_node_16=[$9], cs_ship_addr_sk_node_16=[$10], cs_call_center_sk_node_16=[$11], cs_catalog_page_sk_node_16=[$12], cs_ship_mode_sk_node_16=[$13], cs_warehouse_sk_node_16=[$14], cs_item_sk_node_16=[$15], cs_promo_sk_node_16=[$16], cs_order_number_node_16=[$17], cs_quantity_node_16=[$18], cs_wholesale_cost_node_16=[$19], cs_list_price_node_16=[$20], cs_sales_price_node_16=[$21], cs_ext_discount_amt_node_16=[$22], cs_ext_sales_price_node_16=[$23], cs_ext_wholesale_cost_node_16=[$24], cs_ext_list_price_node_16=[$25], cs_ext_tax_node_16=[$26], cs_coupon_amt_node_16=[$27], cs_ext_ship_cost_node_16=[$28], cs_net_paid_node_16=[$29], cs_net_paid_inc_tax_node_16=[$30], cs_net_paid_inc_ship_node_16=[$31], cs_net_paid_inc_ship_tax_node_16=[$32], cs_net_profit_node_16=[$33])
      :  :           +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
      :  +- LogicalSort(sort0=[$26], dir0=[ASC], fetch=[45])
      :     +- LogicalJoin(condition=[=($31, $10)], joinType=[inner])
      :        :- LogicalProject(s_store_sk_node_17=[$0], s_store_id_node_17=[$1], s_rec_start_date_node_17=[$2], s_rec_end_date_node_17=[$3], s_closed_date_sk_node_17=[$4], s_store_name_node_17=[$5], s_number_employees_node_17=[$6], s_floor_space_node_17=[$7], s_hours_node_17=[$8], s_manager_node_17=[$9], s_market_id_node_17=[$10], s_geography_class_node_17=[$11], s_market_desc_node_17=[$12], s_market_manager_node_17=[$13], s_division_id_node_17=[$14], s_division_name_node_17=[$15], s_company_id_node_17=[$16], s_company_name_node_17=[$17], s_street_number_node_17=[$18], s_street_name_node_17=[$19], s_street_type_node_17=[$20], s_suite_number_node_17=[$21], s_city_node_17=[$22], s_county_node_17=[$23], s_state_node_17=[$24], s_zip_node_17=[$25], s_country_node_17=[$26], s_gmt_offset_node_17=[$27], s_tax_precentage_node_17=[$28])
      :        :  +- LogicalTableScan(table=[[default_catalog, default_database, store]])
      :        +- LogicalProject(ib_income_band_sk_node_18=[$0], ib_lower_bound_node_18=[$1], ib_upper_bound_node_18=[$2])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, income_band]])
      +- LogicalJoin(condition=[=($0, $12)], joinType=[inner])
         :- LogicalProject(ca_street_number_node_19=[$0], _c1=[_UTF-16LE'hello'])
         :  +- LogicalFilter(condition=[>(CHAR_LENGTH($0), 5)])
         :     +- LogicalProject(ca_street_number_node_19=[$2])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
         +- LogicalSort(sort0=[$12], dir0=[ASC], fetch=[33])
            +- LogicalSort(sort0=[$13], dir0=[ASC])
               +- LogicalProject(i_item_sk_node_20=[$0], i_item_id_node_20=[$1], i_rec_start_date_node_20=[$2], i_rec_end_date_node_20=[$3], i_item_desc_node_20=[$4], i_current_price_node_20=[$5], i_wholesale_cost_node_20=[$6], i_brand_id_node_20=[$7], i_brand_node_20=[$8], i_class_id_node_20=[$9], i_class_node_20=[$10], i_category_id_node_20=[$11], i_category_node_20=[$12], i_manufact_id_node_20=[$13], i_manufact_node_20=[$14], i_size_node_20=[$15], i_formulation_node_20=[$16], i_color_node_20=[$17], i_units_node_20=[$18], i_container_node_20=[$19], i_manager_id_node_20=[$20], i_product_name_node_20=[$21])
                  +- LogicalTableScan(table=[[default_catalog, default_database, item]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cs_ext_tax_node_16])
+- HashAggregate(isMerge=[false], groupBy=[i_current_price], select=[i_current_price, MAX(cs_ext_tax_node_16) AS EXPR$0])
   +- Exchange(distribution=[hash[i_current_price]])
      +- HashJoin(joinType=[InnerJoin], where=[=(i_rec_end_date, s_street_number_node_17)], select=[cs_sold_date_sk_node_16, cs_sold_time_sk_node_16, cs_ship_date_sk_node_16, cs_bill_customer_sk_node_16, cs_bill_cdemo_sk_node_16, cs_bill_hdemo_sk_node_16, cs_bill_addr_sk_node_16, cs_ship_customer_sk_node_16, cs_ship_cdemo_sk_node_16, cs_ship_hdemo_sk_node_16, cs_ship_addr_sk_node_16, cs_call_center_sk_node_16, cs_catalog_page_sk_node_16, cs_ship_mode_sk_node_16, cs_warehouse_sk_node_16, cs_item_sk_node_16, cs_promo_sk_node_16, cs_order_number_node_16, cs_quantity_node_16, cs_wholesale_cost_node_16, cs_list_price_node_16, cs_sales_price_node_16, cs_ext_discount_amt_node_16, cs_ext_sales_price_node_16, cs_ext_wholesale_cost_node_16, cs_ext_list_price_node_16, cs_ext_tax_node_16, cs_coupon_amt_node_16, cs_ext_ship_cost_node_16, cs_net_paid_node_16, cs_net_paid_inc_tax_node_16, cs_net_paid_inc_ship_node_16, cs_net_paid_inc_ship_tax_node_16, cs_net_profit_node_16, s_store_sk_node_17, s_store_id_node_17, s_rec_start_date_node_17, s_rec_end_date_node_17, s_closed_date_sk_node_17, s_store_name_node_17, s_number_employees_node_17, s_floor_space_node_17, s_hours_node_17, s_manager_node_17, s_market_id_node_17, s_geography_class_node_17, s_market_desc_node_17, s_market_manager_node_17, s_division_id_node_17, s_division_name_node_17, s_company_id_node_17, s_company_name_node_17, s_street_number_node_17, s_street_name_node_17, s_street_type_node_17, s_suite_number_node_17, s_city_node_17, s_county_node_17, s_state_node_17, s_zip_node_17, s_country_node_17, s_gmt_offset_node_17, s_tax_precentage_node_17, ib_income_band_sk_node_18, ib_lower_bound_node_18, ib_upper_bound_node_18, ca_street_number_node_19, _c1, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], isBroadcast=[true], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[cs_sold_date_sk AS cs_sold_date_sk_node_16, cs_sold_time_sk AS cs_sold_time_sk_node_16, cs_ship_date_sk AS cs_ship_date_sk_node_16, cs_bill_customer_sk AS cs_bill_customer_sk_node_16, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_16, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_16, cs_bill_addr_sk AS cs_bill_addr_sk_node_16, cs_ship_customer_sk AS cs_ship_customer_sk_node_16, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_16, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_16, cs_ship_addr_sk AS cs_ship_addr_sk_node_16, cs_call_center_sk AS cs_call_center_sk_node_16, cs_catalog_page_sk AS cs_catalog_page_sk_node_16, cs_ship_mode_sk AS cs_ship_mode_sk_node_16, cs_warehouse_sk AS cs_warehouse_sk_node_16, cs_item_sk AS cs_item_sk_node_16, cs_promo_sk AS cs_promo_sk_node_16, cs_order_number AS cs_order_number_node_16, cs_quantity AS cs_quantity_node_16, cs_wholesale_cost AS cs_wholesale_cost_node_16, cs_list_price AS cs_list_price_node_16, cs_sales_price AS cs_sales_price_node_16, cs_ext_discount_amt AS cs_ext_discount_amt_node_16, cs_ext_sales_price AS cs_ext_sales_price_node_16, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_16, cs_ext_list_price AS cs_ext_list_price_node_16, cs_ext_tax AS cs_ext_tax_node_16, cs_coupon_amt AS cs_coupon_amt_node_16, cs_ext_ship_cost AS cs_ext_ship_cost_node_16, cs_net_paid AS cs_net_paid_node_16, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_16, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_16, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_16, cs_net_profit AS cs_net_profit_node_16, s_store_sk_node_17, s_store_id_node_17, s_rec_start_date_node_17, s_rec_end_date_node_17, s_closed_date_sk_node_17, s_store_name_node_17, s_number_employees_node_17, s_floor_space_node_17, s_hours_node_17, s_manager_node_17, s_market_id_node_17, s_geography_class_node_17, s_market_desc_node_17, s_market_manager_node_17, s_division_id_node_17, s_division_name_node_17, s_company_id_node_17, s_company_name_node_17, s_street_number_node_17, s_street_name_node_17, s_street_type_node_17, s_suite_number_node_17, s_city_node_17, s_county_node_17, s_state_node_17, s_zip_node_17, s_country_node_17, s_gmt_offset_node_17, s_tax_precentage_node_17, ib_income_band_sk_node_18, ib_lower_bound_node_18, ib_upper_bound_node_18])
         :     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cs_net_paid_inc_tax, s_gmt_offset_node_170)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, s_store_sk_node_17, s_store_id_node_17, s_rec_start_date_node_17, s_rec_end_date_node_17, s_closed_date_sk_node_17, s_store_name_node_17, s_number_employees_node_17, s_floor_space_node_17, s_hours_node_17, s_manager_node_17, s_market_id_node_17, s_geography_class_node_17, s_market_desc_node_17, s_market_manager_node_17, s_division_id_node_17, s_division_name_node_17, s_company_id_node_17, s_company_name_node_17, s_street_number_node_17, s_street_name_node_17, s_street_type_node_17, s_suite_number_node_17, s_city_node_17, s_county_node_17, s_state_node_17, s_zip_node_17, s_country_node_17, s_gmt_offset_node_17, s_tax_precentage_node_17, ib_income_band_sk_node_18, ib_lower_bound_node_18, ib_upper_bound_node_18, s_gmt_offset_node_170], build=[right])
         :        :- Sort(orderBy=[cs_ext_discount_amt ASC])
         :        :  +- Limit(offset=[0], fetch=[75], global=[true])
         :        :     +- Exchange(distribution=[single])
         :        :        +- Limit(offset=[0], fetch=[75], global=[false])
         :        :           +- Calc(select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], where=[>=(cs_call_center_sk, 34)])
         :        :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales, filter=[>=(cs_call_center_sk, 34)]]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
         :        +- Exchange(distribution=[broadcast])
         :           +- Calc(select=[s_store_sk AS s_store_sk_node_17, s_store_id AS s_store_id_node_17, s_rec_start_date AS s_rec_start_date_node_17, s_rec_end_date AS s_rec_end_date_node_17, s_closed_date_sk AS s_closed_date_sk_node_17, s_store_name AS s_store_name_node_17, s_number_employees AS s_number_employees_node_17, s_floor_space AS s_floor_space_node_17, s_hours AS s_hours_node_17, s_manager AS s_manager_node_17, s_market_id AS s_market_id_node_17, s_geography_class AS s_geography_class_node_17, s_market_desc AS s_market_desc_node_17, s_market_manager AS s_market_manager_node_17, s_division_id AS s_division_id_node_17, s_division_name AS s_division_name_node_17, s_company_id AS s_company_id_node_17, s_company_name AS s_company_name_node_17, s_street_number AS s_street_number_node_17, s_street_name AS s_street_name_node_17, s_street_type AS s_street_type_node_17, s_suite_number AS s_suite_number_node_17, s_city AS s_city_node_17, s_county AS s_county_node_17, s_state AS s_state_node_17, s_zip AS s_zip_node_17, s_country AS s_country_node_17, s_gmt_offset AS s_gmt_offset_node_17, s_tax_precentage AS s_tax_precentage_node_17, ib_income_band_sk AS ib_income_band_sk_node_18, ib_lower_bound AS ib_lower_bound_node_18, ib_upper_bound AS ib_upper_bound_node_18, CAST(s_gmt_offset AS DECIMAL(7, 2)) AS s_gmt_offset_node_170])
         :              +- SortLimit(orderBy=[s_country ASC], offset=[0], fetch=[45], global=[true])
         :                 +- Exchange(distribution=[single])
         :                    +- SortLimit(orderBy=[s_country ASC], offset=[0], fetch=[45], global=[false])
         :                       +- HashJoin(joinType=[InnerJoin], where=[=(ib_upper_bound, s_market_id)], select=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, ib_income_band_sk, ib_lower_bound, ib_upper_bound], isBroadcast=[true], build=[right])
         :                          :- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
         :                          +- Exchange(distribution=[broadcast])
         :                             +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
         +- HashJoin(joinType=[InnerJoin], where=[=(ca_street_number_node_19, i_class)], select=[ca_street_number_node_19, _c1, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], isBroadcast=[true], build=[right])
            :- Calc(select=[ca_street_number AS ca_street_number_node_19, 'hello' AS _c1], where=[>(CHAR_LENGTH(ca_street_number), 5)])
            :  +- TableSourceScan(table=[[default_catalog, default_database, customer_address, filter=[>(CHAR_LENGTH(ca_street_number), 5)], project=[ca_street_number], metadata=[]]], fields=[ca_street_number])
            +- Exchange(distribution=[broadcast])
               +- SortLimit(orderBy=[i_category ASC], offset=[0], fetch=[33], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[i_category ASC], offset=[0], fetch=[33], global=[false])
                        +- Sort(orderBy=[i_manufact_id ASC])
                           +- Exchange(distribution=[single])
                              +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cs_ext_tax_node_16])
+- HashAggregate(isMerge=[false], groupBy=[i_current_price], select=[i_current_price, MAX(cs_ext_tax_node_16) AS EXPR$0])
   +- Exchange(distribution=[hash[i_current_price]])
      +- MultipleInput(readOrder=[0,0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(i_rec_end_date = s_street_number_node_17)], select=[cs_sold_date_sk_node_16, cs_sold_time_sk_node_16, cs_ship_date_sk_node_16, cs_bill_customer_sk_node_16, cs_bill_cdemo_sk_node_16, cs_bill_hdemo_sk_node_16, cs_bill_addr_sk_node_16, cs_ship_customer_sk_node_16, cs_ship_cdemo_sk_node_16, cs_ship_hdemo_sk_node_16, cs_ship_addr_sk_node_16, cs_call_center_sk_node_16, cs_catalog_page_sk_node_16, cs_ship_mode_sk_node_16, cs_warehouse_sk_node_16, cs_item_sk_node_16, cs_promo_sk_node_16, cs_order_number_node_16, cs_quantity_node_16, cs_wholesale_cost_node_16, cs_list_price_node_16, cs_sales_price_node_16, cs_ext_discount_amt_node_16, cs_ext_sales_price_node_16, cs_ext_wholesale_cost_node_16, cs_ext_list_price_node_16, cs_ext_tax_node_16, cs_coupon_amt_node_16, cs_ext_ship_cost_node_16, cs_net_paid_node_16, cs_net_paid_inc_tax_node_16, cs_net_paid_inc_ship_node_16, cs_net_paid_inc_ship_tax_node_16, cs_net_profit_node_16, s_store_sk_node_17, s_store_id_node_17, s_rec_start_date_node_17, s_rec_end_date_node_17, s_closed_date_sk_node_17, s_store_name_node_17, s_number_employees_node_17, s_floor_space_node_17, s_hours_node_17, s_manager_node_17, s_market_id_node_17, s_geography_class_node_17, s_market_desc_node_17, s_market_manager_node_17, s_division_id_node_17, s_division_name_node_17, s_company_id_node_17, s_company_name_node_17, s_street_number_node_17, s_street_name_node_17, s_street_type_node_17, s_suite_number_node_17, s_city_node_17, s_county_node_17, s_state_node_17, s_zip_node_17, s_country_node_17, s_gmt_offset_node_17, s_tax_precentage_node_17, ib_income_band_sk_node_18, ib_lower_bound_node_18, ib_upper_bound_node_18, ca_street_number_node_19, _c1, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- HashJoin(joinType=[InnerJoin], where=[(ca_street_number_node_19 = i_class)], select=[ca_street_number_node_19, _c1, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], isBroadcast=[true], build=[right])\
   :- Calc(select=[ca_street_number AS ca_street_number_node_19, 'hello' AS _c1], where=[(CHAR_LENGTH(ca_street_number) > 5)])\
   :  +- [#3] TableSourceScan(table=[[default_catalog, default_database, customer_address, filter=[>(CHAR_LENGTH(ca_street_number), 5)], project=[ca_street_number], metadata=[]]], fields=[ca_street_number])\
   +- [#2] Exchange(distribution=[broadcast])\
])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[cs_sold_date_sk AS cs_sold_date_sk_node_16, cs_sold_time_sk AS cs_sold_time_sk_node_16, cs_ship_date_sk AS cs_ship_date_sk_node_16, cs_bill_customer_sk AS cs_bill_customer_sk_node_16, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_16, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_16, cs_bill_addr_sk AS cs_bill_addr_sk_node_16, cs_ship_customer_sk AS cs_ship_customer_sk_node_16, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_16, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_16, cs_ship_addr_sk AS cs_ship_addr_sk_node_16, cs_call_center_sk AS cs_call_center_sk_node_16, cs_catalog_page_sk AS cs_catalog_page_sk_node_16, cs_ship_mode_sk AS cs_ship_mode_sk_node_16, cs_warehouse_sk AS cs_warehouse_sk_node_16, cs_item_sk AS cs_item_sk_node_16, cs_promo_sk AS cs_promo_sk_node_16, cs_order_number AS cs_order_number_node_16, cs_quantity AS cs_quantity_node_16, cs_wholesale_cost AS cs_wholesale_cost_node_16, cs_list_price AS cs_list_price_node_16, cs_sales_price AS cs_sales_price_node_16, cs_ext_discount_amt AS cs_ext_discount_amt_node_16, cs_ext_sales_price AS cs_ext_sales_price_node_16, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_16, cs_ext_list_price AS cs_ext_list_price_node_16, cs_ext_tax AS cs_ext_tax_node_16, cs_coupon_amt AS cs_coupon_amt_node_16, cs_ext_ship_cost AS cs_ext_ship_cost_node_16, cs_net_paid AS cs_net_paid_node_16, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_16, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_16, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_16, cs_net_profit AS cs_net_profit_node_16, s_store_sk_node_17, s_store_id_node_17, s_rec_start_date_node_17, s_rec_end_date_node_17, s_closed_date_sk_node_17, s_store_name_node_17, s_number_employees_node_17, s_floor_space_node_17, s_hours_node_17, s_manager_node_17, s_market_id_node_17, s_geography_class_node_17, s_market_desc_node_17, s_market_manager_node_17, s_division_id_node_17, s_division_name_node_17, s_company_id_node_17, s_company_name_node_17, s_street_number_node_17, s_street_name_node_17, s_street_type_node_17, s_suite_number_node_17, s_city_node_17, s_county_node_17, s_state_node_17, s_zip_node_17, s_country_node_17, s_gmt_offset_node_17, s_tax_precentage_node_17, ib_income_band_sk_node_18, ib_lower_bound_node_18, ib_upper_bound_node_18])
         :     +- NestedLoopJoin(joinType=[InnerJoin], where=[(cs_net_paid_inc_tax = s_gmt_offset_node_170)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, s_store_sk_node_17, s_store_id_node_17, s_rec_start_date_node_17, s_rec_end_date_node_17, s_closed_date_sk_node_17, s_store_name_node_17, s_number_employees_node_17, s_floor_space_node_17, s_hours_node_17, s_manager_node_17, s_market_id_node_17, s_geography_class_node_17, s_market_desc_node_17, s_market_manager_node_17, s_division_id_node_17, s_division_name_node_17, s_company_id_node_17, s_company_name_node_17, s_street_number_node_17, s_street_name_node_17, s_street_type_node_17, s_suite_number_node_17, s_city_node_17, s_county_node_17, s_state_node_17, s_zip_node_17, s_country_node_17, s_gmt_offset_node_17, s_tax_precentage_node_17, ib_income_band_sk_node_18, ib_lower_bound_node_18, ib_upper_bound_node_18, s_gmt_offset_node_170], build=[right])
         :        :- Sort(orderBy=[cs_ext_discount_amt ASC])
         :        :  +- Limit(offset=[0], fetch=[75], global=[true])
         :        :     +- Exchange(distribution=[single])
         :        :        +- Limit(offset=[0], fetch=[75], global=[false])
         :        :           +- Calc(select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], where=[(cs_call_center_sk >= 34)])
         :        :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales, filter=[>=(cs_call_center_sk, 34)]]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
         :        +- Exchange(distribution=[broadcast])
         :           +- Calc(select=[s_store_sk AS s_store_sk_node_17, s_store_id AS s_store_id_node_17, s_rec_start_date AS s_rec_start_date_node_17, s_rec_end_date AS s_rec_end_date_node_17, s_closed_date_sk AS s_closed_date_sk_node_17, s_store_name AS s_store_name_node_17, s_number_employees AS s_number_employees_node_17, s_floor_space AS s_floor_space_node_17, s_hours AS s_hours_node_17, s_manager AS s_manager_node_17, s_market_id AS s_market_id_node_17, s_geography_class AS s_geography_class_node_17, s_market_desc AS s_market_desc_node_17, s_market_manager AS s_market_manager_node_17, s_division_id AS s_division_id_node_17, s_division_name AS s_division_name_node_17, s_company_id AS s_company_id_node_17, s_company_name AS s_company_name_node_17, s_street_number AS s_street_number_node_17, s_street_name AS s_street_name_node_17, s_street_type AS s_street_type_node_17, s_suite_number AS s_suite_number_node_17, s_city AS s_city_node_17, s_county AS s_county_node_17, s_state AS s_state_node_17, s_zip AS s_zip_node_17, s_country AS s_country_node_17, s_gmt_offset AS s_gmt_offset_node_17, s_tax_precentage AS s_tax_precentage_node_17, ib_income_band_sk AS ib_income_band_sk_node_18, ib_lower_bound AS ib_lower_bound_node_18, ib_upper_bound AS ib_upper_bound_node_18, CAST(s_gmt_offset AS DECIMAL(7, 2)) AS s_gmt_offset_node_170])
         :              +- SortLimit(orderBy=[s_country ASC], offset=[0], fetch=[45], global=[true])
         :                 +- Exchange(distribution=[single])
         :                    +- SortLimit(orderBy=[s_country ASC], offset=[0], fetch=[45], global=[false])
         :                       +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(ib_upper_bound = s_market_id)], select=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, ib_income_band_sk, ib_lower_bound, ib_upper_bound], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])\
+- [#2] Exchange(distribution=[broadcast])\
])
         :                          :- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
         :                          +- Exchange(distribution=[broadcast])
         :                             +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
         :- Exchange(distribution=[broadcast])
         :  +- SortLimit(orderBy=[i_category ASC], offset=[0], fetch=[33], global=[true])
         :     +- Exchange(distribution=[single])
         :        +- SortLimit(orderBy=[i_category ASC], offset=[0], fetch=[33], global=[false])
         :           +- Sort(orderBy=[i_manufact_id ASC])
         :              +- Exchange(distribution=[single])
         :                 +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
         +- TableSourceScan(table=[[default_catalog, default_database, customer_address, filter=[>(CHAR_LENGTH(ca_street_number), 5)], project=[ca_street_number], metadata=[]]], fields=[ca_street_number])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o316037705.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#636810060:AbstractConverter.BATCH_PHYSICAL.hash[0, 1, 2]true.[12](input=RelSubset#636810058,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1, 2]true,sort=[12]), rel#636810057:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[12](input=RelSubset#636810056,groupBy=i_rec_end_date, i_current_price, i_class,select=i_rec_end_date, i_current_price, i_class)]
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