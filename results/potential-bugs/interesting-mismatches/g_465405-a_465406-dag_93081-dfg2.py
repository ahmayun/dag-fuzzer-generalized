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
    return len(values) / (1.0 / values).sum() if (values != 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_12 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_14 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_10 = autonode_12.join(autonode_13, col('inv_date_sk_node_12') == col('ss_addr_sk_node_13'))
autonode_11 = autonode_14.distinct()
autonode_8 = autonode_10.filter(preloaded_udf_boolean(col('ss_ext_sales_price_node_13')))
autonode_9 = autonode_11.order_by(col('web_gmt_offset_node_14'))
autonode_6 = autonode_8.order_by(col('ss_item_sk_node_13'))
autonode_7 = autonode_9.add_columns(lit("hello"))
autonode_4 = autonode_6.limit(84)
autonode_5 = autonode_7.order_by(col('web_rec_end_date_node_14'))
autonode_2 = autonode_4.alias('PRCv2')
autonode_3 = autonode_5.order_by(col('web_zip_node_14'))
autonode_1 = autonode_2.join(autonode_3, col('ss_coupon_amt_node_13') == col('web_gmt_offset_node_14'))
sink = autonode_1.group_by(col('web_country_node_14')).select(col('ss_sold_date_sk_node_13').sum.alias('ss_sold_date_sk_node_13'))
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
LogicalProject(ss_sold_date_sk_node_13=[$1])
+- LogicalAggregate(group=[{50}], EXPR$0=[SUM($4)])
   +- LogicalJoin(condition=[=($23, $51)], joinType=[inner])
      :- LogicalProject(PRCv2=[AS($0, _UTF-16LE'PRCv2')], inv_item_sk_node_12=[$1], inv_warehouse_sk_node_12=[$2], inv_quantity_on_hand_node_12=[$3], ss_sold_date_sk_node_13=[$4], ss_sold_time_sk_node_13=[$5], ss_item_sk_node_13=[$6], ss_customer_sk_node_13=[$7], ss_cdemo_sk_node_13=[$8], ss_hdemo_sk_node_13=[$9], ss_addr_sk_node_13=[$10], ss_store_sk_node_13=[$11], ss_promo_sk_node_13=[$12], ss_ticket_number_node_13=[$13], ss_quantity_node_13=[$14], ss_wholesale_cost_node_13=[$15], ss_list_price_node_13=[$16], ss_sales_price_node_13=[$17], ss_ext_discount_amt_node_13=[$18], ss_ext_sales_price_node_13=[$19], ss_ext_wholesale_cost_node_13=[$20], ss_ext_list_price_node_13=[$21], ss_ext_tax_node_13=[$22], ss_coupon_amt_node_13=[$23], ss_net_paid_node_13=[$24], ss_net_paid_inc_tax_node_13=[$25], ss_net_profit_node_13=[$26])
      :  +- LogicalSort(sort0=[$6], dir0=[ASC], fetch=[84])
      :     +- LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($19)])
      :        +- LogicalJoin(condition=[=($0, $10)], joinType=[inner])
      :           :- LogicalProject(inv_date_sk_node_12=[$0], inv_item_sk_node_12=[$1], inv_warehouse_sk_node_12=[$2], inv_quantity_on_hand_node_12=[$3])
      :           :  +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])
      :           +- LogicalProject(ss_sold_date_sk_node_13=[$0], ss_sold_time_sk_node_13=[$1], ss_item_sk_node_13=[$2], ss_customer_sk_node_13=[$3], ss_cdemo_sk_node_13=[$4], ss_hdemo_sk_node_13=[$5], ss_addr_sk_node_13=[$6], ss_store_sk_node_13=[$7], ss_promo_sk_node_13=[$8], ss_ticket_number_node_13=[$9], ss_quantity_node_13=[$10], ss_wholesale_cost_node_13=[$11], ss_list_price_node_13=[$12], ss_sales_price_node_13=[$13], ss_ext_discount_amt_node_13=[$14], ss_ext_sales_price_node_13=[$15], ss_ext_wholesale_cost_node_13=[$16], ss_ext_list_price_node_13=[$17], ss_ext_tax_node_13=[$18], ss_coupon_amt_node_13=[$19], ss_net_paid_node_13=[$20], ss_net_paid_inc_tax_node_13=[$21], ss_net_profit_node_13=[$22])
      :              +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalSort(sort0=[$22], dir0=[ASC])
         +- LogicalSort(sort0=[$3], dir0=[ASC])
            +- LogicalProject(web_site_sk_node_14=[$0], web_site_id_node_14=[$1], web_rec_start_date_node_14=[$2], web_rec_end_date_node_14=[$3], web_name_node_14=[$4], web_open_date_sk_node_14=[$5], web_close_date_sk_node_14=[$6], web_class_node_14=[$7], web_manager_node_14=[$8], web_mkt_id_node_14=[$9], web_mkt_class_node_14=[$10], web_mkt_desc_node_14=[$11], web_market_manager_node_14=[$12], web_company_id_node_14=[$13], web_company_name_node_14=[$14], web_street_number_node_14=[$15], web_street_name_node_14=[$16], web_street_type_node_14=[$17], web_suite_number_node_14=[$18], web_city_node_14=[$19], web_county_node_14=[$20], web_state_node_14=[$21], web_zip_node_14=[$22], web_country_node_14=[$23], web_gmt_offset_node_14=[$24], web_tax_percentage_node_14=[$25], _c26=[_UTF-16LE'hello'])
               +- LogicalSort(sort0=[$24], dir0=[ASC])
                  +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}])
                     +- LogicalProject(web_site_sk_node_14=[$0], web_site_id_node_14=[$1], web_rec_start_date_node_14=[$2], web_rec_end_date_node_14=[$3], web_name_node_14=[$4], web_open_date_sk_node_14=[$5], web_close_date_sk_node_14=[$6], web_class_node_14=[$7], web_manager_node_14=[$8], web_mkt_id_node_14=[$9], web_mkt_class_node_14=[$10], web_mkt_desc_node_14=[$11], web_market_manager_node_14=[$12], web_company_id_node_14=[$13], web_company_name_node_14=[$14], web_street_number_node_14=[$15], web_street_name_node_14=[$16], web_street_type_node_14=[$17], web_suite_number_node_14=[$18], web_city_node_14=[$19], web_county_node_14=[$20], web_state_node_14=[$21], web_zip_node_14=[$22], web_country_node_14=[$23], web_gmt_offset_node_14=[$24], web_tax_percentage_node_14=[$25])
                        +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ss_sold_date_sk_node_13])
+- HashAggregate(isMerge=[false], groupBy=[web_country_node_14], select=[web_country_node_14, SUM(ss_sold_date_sk_node_13) AS EXPR$0])
   +- Exchange(distribution=[hash[web_country_node_14]])
      +- Calc(select=[PRCv2, inv_item_sk_node_12, inv_warehouse_sk_node_12, inv_quantity_on_hand_node_12, ss_sold_date_sk_node_13, ss_sold_time_sk_node_13, ss_item_sk_node_13, ss_customer_sk_node_13, ss_cdemo_sk_node_13, ss_hdemo_sk_node_13, ss_addr_sk_node_13, ss_store_sk_node_13, ss_promo_sk_node_13, ss_ticket_number_node_13, ss_quantity_node_13, ss_wholesale_cost_node_13, ss_list_price_node_13, ss_sales_price_node_13, ss_ext_discount_amt_node_13, ss_ext_sales_price_node_13, ss_ext_wholesale_cost_node_13, ss_ext_list_price_node_13, ss_ext_tax_node_13, ss_coupon_amt_node_13, ss_net_paid_node_13, ss_net_paid_inc_tax_node_13, ss_net_profit_node_13, web_site_sk_node_14, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14, 'hello' AS _c26])
         +- HashJoin(joinType=[InnerJoin], where=[=(ss_coupon_amt_node_13, web_gmt_offset_node_140)], select=[PRCv2, inv_item_sk_node_12, inv_warehouse_sk_node_12, inv_quantity_on_hand_node_12, ss_sold_date_sk_node_13, ss_sold_time_sk_node_13, ss_item_sk_node_13, ss_customer_sk_node_13, ss_cdemo_sk_node_13, ss_hdemo_sk_node_13, ss_addr_sk_node_13, ss_store_sk_node_13, ss_promo_sk_node_13, ss_ticket_number_node_13, ss_quantity_node_13, ss_wholesale_cost_node_13, ss_list_price_node_13, ss_sales_price_node_13, ss_ext_discount_amt_node_13, ss_ext_sales_price_node_13, ss_ext_wholesale_cost_node_13, ss_ext_list_price_node_13, ss_ext_tax_node_13, ss_coupon_amt_node_13, ss_net_paid_node_13, ss_net_paid_inc_tax_node_13, ss_net_profit_node_13, web_site_sk_node_14, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14, web_gmt_offset_node_140], isBroadcast=[true], build=[right])
            :- Calc(select=[inv_date_sk AS PRCv2, inv_item_sk AS inv_item_sk_node_12, inv_warehouse_sk AS inv_warehouse_sk_node_12, inv_quantity_on_hand AS inv_quantity_on_hand_node_12, ss_sold_date_sk AS ss_sold_date_sk_node_13, ss_sold_time_sk AS ss_sold_time_sk_node_13, ss_item_sk AS ss_item_sk_node_13, ss_customer_sk AS ss_customer_sk_node_13, ss_cdemo_sk AS ss_cdemo_sk_node_13, ss_hdemo_sk AS ss_hdemo_sk_node_13, ss_addr_sk AS ss_addr_sk_node_13, ss_store_sk AS ss_store_sk_node_13, ss_promo_sk AS ss_promo_sk_node_13, ss_ticket_number AS ss_ticket_number_node_13, ss_quantity AS ss_quantity_node_13, ss_wholesale_cost AS ss_wholesale_cost_node_13, ss_list_price AS ss_list_price_node_13, ss_sales_price AS ss_sales_price_node_13, ss_ext_discount_amt AS ss_ext_discount_amt_node_13, ss_ext_sales_price AS ss_ext_sales_price_node_13, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_13, ss_ext_list_price AS ss_ext_list_price_node_13, ss_ext_tax AS ss_ext_tax_node_13, ss_coupon_amt AS ss_coupon_amt_node_13, ss_net_paid AS ss_net_paid_node_13, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_13, ss_net_profit AS ss_net_profit_node_13])
            :  +- SortLimit(orderBy=[ss_item_sk ASC], offset=[0], fetch=[84], global=[true])
            :     +- Exchange(distribution=[single])
            :        +- SortLimit(orderBy=[ss_item_sk ASC], offset=[0], fetch=[84], global=[false])
            :           +- HashJoin(joinType=[InnerJoin], where=[=(inv_date_sk, ss_addr_sk)], select=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[right])
            :              :- Exchange(distribution=[hash[inv_date_sk]])
            :              :  +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
            :              +- Exchange(distribution=[hash[ss_addr_sk]])
            :                 +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[f0])
            :                    +- PythonCalc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(ss_ext_sales_price) AS f0])
            :                       +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[web_site_sk AS web_site_sk_node_14, web_site_id AS web_site_id_node_14, web_rec_start_date AS web_rec_start_date_node_14, web_rec_end_date AS web_rec_end_date_node_14, web_name AS web_name_node_14, web_open_date_sk AS web_open_date_sk_node_14, web_close_date_sk AS web_close_date_sk_node_14, web_class AS web_class_node_14, web_manager AS web_manager_node_14, web_mkt_id AS web_mkt_id_node_14, web_mkt_class AS web_mkt_class_node_14, web_mkt_desc AS web_mkt_desc_node_14, web_market_manager AS web_market_manager_node_14, web_company_id AS web_company_id_node_14, web_company_name AS web_company_name_node_14, web_street_number AS web_street_number_node_14, web_street_name AS web_street_name_node_14, web_street_type AS web_street_type_node_14, web_suite_number AS web_suite_number_node_14, web_city AS web_city_node_14, web_county AS web_county_node_14, web_state AS web_state_node_14, web_zip AS web_zip_node_14, web_country AS web_country_node_14, web_gmt_offset AS web_gmt_offset_node_14, web_tax_percentage AS web_tax_percentage_node_14, CAST(web_gmt_offset AS DECIMAL(7, 2)) AS web_gmt_offset_node_140])
                  +- Sort(orderBy=[web_zip ASC])
                     +- Sort(orderBy=[web_rec_end_date ASC])
                        +- Sort(orderBy=[web_gmt_offset ASC])
                           +- Exchange(distribution=[single])
                              +- HashAggregate(isMerge=[false], groupBy=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                                 +- Exchange(distribution=[hash[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage]])
                                    +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ss_sold_date_sk_node_13])
+- HashAggregate(isMerge=[false], groupBy=[web_country_node_14], select=[web_country_node_14, SUM(ss_sold_date_sk_node_13) AS EXPR$0])
   +- Exchange(distribution=[hash[web_country_node_14]])
      +- Calc(select=[PRCv2, inv_item_sk_node_12, inv_warehouse_sk_node_12, inv_quantity_on_hand_node_12, ss_sold_date_sk_node_13, ss_sold_time_sk_node_13, ss_item_sk_node_13, ss_customer_sk_node_13, ss_cdemo_sk_node_13, ss_hdemo_sk_node_13, ss_addr_sk_node_13, ss_store_sk_node_13, ss_promo_sk_node_13, ss_ticket_number_node_13, ss_quantity_node_13, ss_wholesale_cost_node_13, ss_list_price_node_13, ss_sales_price_node_13, ss_ext_discount_amt_node_13, ss_ext_sales_price_node_13, ss_ext_wholesale_cost_node_13, ss_ext_list_price_node_13, ss_ext_tax_node_13, ss_coupon_amt_node_13, ss_net_paid_node_13, ss_net_paid_inc_tax_node_13, ss_net_profit_node_13, web_site_sk_node_14, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14, 'hello' AS _c26])
         +- HashJoin(joinType=[InnerJoin], where=[(ss_coupon_amt_node_13 = web_gmt_offset_node_140)], select=[PRCv2, inv_item_sk_node_12, inv_warehouse_sk_node_12, inv_quantity_on_hand_node_12, ss_sold_date_sk_node_13, ss_sold_time_sk_node_13, ss_item_sk_node_13, ss_customer_sk_node_13, ss_cdemo_sk_node_13, ss_hdemo_sk_node_13, ss_addr_sk_node_13, ss_store_sk_node_13, ss_promo_sk_node_13, ss_ticket_number_node_13, ss_quantity_node_13, ss_wholesale_cost_node_13, ss_list_price_node_13, ss_sales_price_node_13, ss_ext_discount_amt_node_13, ss_ext_sales_price_node_13, ss_ext_wholesale_cost_node_13, ss_ext_list_price_node_13, ss_ext_tax_node_13, ss_coupon_amt_node_13, ss_net_paid_node_13, ss_net_paid_inc_tax_node_13, ss_net_profit_node_13, web_site_sk_node_14, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14, web_gmt_offset_node_140], isBroadcast=[true], build=[right])
            :- Calc(select=[inv_date_sk AS PRCv2, inv_item_sk AS inv_item_sk_node_12, inv_warehouse_sk AS inv_warehouse_sk_node_12, inv_quantity_on_hand AS inv_quantity_on_hand_node_12, ss_sold_date_sk AS ss_sold_date_sk_node_13, ss_sold_time_sk AS ss_sold_time_sk_node_13, ss_item_sk AS ss_item_sk_node_13, ss_customer_sk AS ss_customer_sk_node_13, ss_cdemo_sk AS ss_cdemo_sk_node_13, ss_hdemo_sk AS ss_hdemo_sk_node_13, ss_addr_sk AS ss_addr_sk_node_13, ss_store_sk AS ss_store_sk_node_13, ss_promo_sk AS ss_promo_sk_node_13, ss_ticket_number AS ss_ticket_number_node_13, ss_quantity AS ss_quantity_node_13, ss_wholesale_cost AS ss_wholesale_cost_node_13, ss_list_price AS ss_list_price_node_13, ss_sales_price AS ss_sales_price_node_13, ss_ext_discount_amt AS ss_ext_discount_amt_node_13, ss_ext_sales_price AS ss_ext_sales_price_node_13, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_13, ss_ext_list_price AS ss_ext_list_price_node_13, ss_ext_tax AS ss_ext_tax_node_13, ss_coupon_amt AS ss_coupon_amt_node_13, ss_net_paid AS ss_net_paid_node_13, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_13, ss_net_profit AS ss_net_profit_node_13])
            :  +- SortLimit(orderBy=[ss_item_sk ASC], offset=[0], fetch=[84], global=[true])
            :     +- Exchange(distribution=[single])
            :        +- SortLimit(orderBy=[ss_item_sk ASC], offset=[0], fetch=[84], global=[false])
            :           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(inv_date_sk = ss_addr_sk)], select=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[right])
            :              :- Exchange(distribution=[hash[inv_date_sk]])
            :              :  +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
            :              +- Exchange(distribution=[hash[ss_addr_sk]])
            :                 +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[f0])
            :                    +- PythonCalc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(ss_ext_sales_price) AS f0])
            :                       +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[web_site_sk AS web_site_sk_node_14, web_site_id AS web_site_id_node_14, web_rec_start_date AS web_rec_start_date_node_14, web_rec_end_date AS web_rec_end_date_node_14, web_name AS web_name_node_14, web_open_date_sk AS web_open_date_sk_node_14, web_close_date_sk AS web_close_date_sk_node_14, web_class AS web_class_node_14, web_manager AS web_manager_node_14, web_mkt_id AS web_mkt_id_node_14, web_mkt_class AS web_mkt_class_node_14, web_mkt_desc AS web_mkt_desc_node_14, web_market_manager AS web_market_manager_node_14, web_company_id AS web_company_id_node_14, web_company_name AS web_company_name_node_14, web_street_number AS web_street_number_node_14, web_street_name AS web_street_name_node_14, web_street_type AS web_street_type_node_14, web_suite_number AS web_suite_number_node_14, web_city AS web_city_node_14, web_county AS web_county_node_14, web_state AS web_state_node_14, web_zip AS web_zip_node_14, web_country AS web_country_node_14, web_gmt_offset AS web_gmt_offset_node_14, web_tax_percentage AS web_tax_percentage_node_14, CAST(web_gmt_offset AS DECIMAL(7, 2)) AS web_gmt_offset_node_140])
                  +- Sort(orderBy=[web_zip ASC])
                     +- Sort(orderBy=[web_rec_end_date ASC])
                        +- Sort(orderBy=[web_gmt_offset ASC])
                           +- Exchange(distribution=[single])
                              +- HashAggregate(isMerge=[false], groupBy=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                                 +- Exchange(distribution=[hash[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage]])
                                    +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o253430146.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#512458071:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[6](input=RelSubset#512458069,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[6]), rel#512458068:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[6](input=RelSubset#512458067,groupBy=ss_coupon_amt,select=ss_coupon_amt, Partial_SUM(ss_sold_date_sk) AS sum$0)]
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