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
    return values.skew()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_9 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_11 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_7 = autonode_9.alias('oxhhL')
autonode_8 = autonode_10.join(autonode_11, col('s_closed_date_sk_node_11') == col('sr_customer_sk_node_10'))
autonode_5 = autonode_7.order_by(col('ws_ship_cdemo_sk_node_9'))
autonode_6 = autonode_8.group_by(col('sr_return_quantity_node_10')).select(col('s_gmt_offset_node_11').max.alias('s_gmt_offset_node_11'))
autonode_4 = autonode_5.join(autonode_6, col('s_gmt_offset_node_11') == col('ws_ext_tax_node_9'))
autonode_3 = autonode_4.group_by(col('ws_sold_time_sk_node_9')).select(col('ws_web_page_sk_node_9').min.alias('ws_web_page_sk_node_9'))
autonode_2 = autonode_3.filter(col('ws_web_page_sk_node_9') > 19)
autonode_1 = autonode_2.distinct()
sink = autonode_1.order_by(col('ws_web_page_sk_node_9'))
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
LogicalSort(sort0=[$0], dir0=[ASC])
+- LogicalAggregate(group=[{0}])
   +- LogicalFilter(condition=[>($0, 19)])
      +- LogicalProject(ws_web_page_sk_node_9=[$1])
         +- LogicalAggregate(group=[{1}], EXPR$0=[MIN($12)])
            +- LogicalJoin(condition=[=($34, $26)], joinType=[inner])
               :- LogicalSort(sort0=[$9], dir0=[ASC])
               :  +- LogicalProject(oxhhL=[AS($0, _UTF-16LE'oxhhL')], ws_sold_time_sk_node_9=[$1], ws_ship_date_sk_node_9=[$2], ws_item_sk_node_9=[$3], ws_bill_customer_sk_node_9=[$4], ws_bill_cdemo_sk_node_9=[$5], ws_bill_hdemo_sk_node_9=[$6], ws_bill_addr_sk_node_9=[$7], ws_ship_customer_sk_node_9=[$8], ws_ship_cdemo_sk_node_9=[$9], ws_ship_hdemo_sk_node_9=[$10], ws_ship_addr_sk_node_9=[$11], ws_web_page_sk_node_9=[$12], ws_web_site_sk_node_9=[$13], ws_ship_mode_sk_node_9=[$14], ws_warehouse_sk_node_9=[$15], ws_promo_sk_node_9=[$16], ws_order_number_node_9=[$17], ws_quantity_node_9=[$18], ws_wholesale_cost_node_9=[$19], ws_list_price_node_9=[$20], ws_sales_price_node_9=[$21], ws_ext_discount_amt_node_9=[$22], ws_ext_sales_price_node_9=[$23], ws_ext_wholesale_cost_node_9=[$24], ws_ext_list_price_node_9=[$25], ws_ext_tax_node_9=[$26], ws_coupon_amt_node_9=[$27], ws_ext_ship_cost_node_9=[$28], ws_net_paid_node_9=[$29], ws_net_paid_inc_tax_node_9=[$30], ws_net_paid_inc_ship_node_9=[$31], ws_net_paid_inc_ship_tax_node_9=[$32], ws_net_profit_node_9=[$33])
               :     +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
               +- LogicalProject(s_gmt_offset_node_11=[$1])
                  +- LogicalAggregate(group=[{10}], EXPR$0=[MAX($47)])
                     +- LogicalJoin(condition=[=($24, $3)], joinType=[inner])
                        :- LogicalProject(sr_returned_date_sk_node_10=[$0], sr_return_time_sk_node_10=[$1], sr_item_sk_node_10=[$2], sr_customer_sk_node_10=[$3], sr_cdemo_sk_node_10=[$4], sr_hdemo_sk_node_10=[$5], sr_addr_sk_node_10=[$6], sr_store_sk_node_10=[$7], sr_reason_sk_node_10=[$8], sr_ticket_number_node_10=[$9], sr_return_quantity_node_10=[$10], sr_return_amt_node_10=[$11], sr_return_tax_node_10=[$12], sr_return_amt_inc_tax_node_10=[$13], sr_fee_node_10=[$14], sr_return_ship_cost_node_10=[$15], sr_refunded_cash_node_10=[$16], sr_reversed_charge_node_10=[$17], sr_store_credit_node_10=[$18], sr_net_loss_node_10=[$19])
                        :  +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
                        +- LogicalProject(s_store_sk_node_11=[$0], s_store_id_node_11=[$1], s_rec_start_date_node_11=[$2], s_rec_end_date_node_11=[$3], s_closed_date_sk_node_11=[$4], s_store_name_node_11=[$5], s_number_employees_node_11=[$6], s_floor_space_node_11=[$7], s_hours_node_11=[$8], s_manager_node_11=[$9], s_market_id_node_11=[$10], s_geography_class_node_11=[$11], s_market_desc_node_11=[$12], s_market_manager_node_11=[$13], s_division_id_node_11=[$14], s_division_name_node_11=[$15], s_company_id_node_11=[$16], s_company_name_node_11=[$17], s_street_number_node_11=[$18], s_street_name_node_11=[$19], s_street_type_node_11=[$20], s_suite_number_node_11=[$21], s_city_node_11=[$22], s_county_node_11=[$23], s_state_node_11=[$24], s_zip_node_11=[$25], s_country_node_11=[$26], s_gmt_offset_node_11=[$27], s_tax_precentage_node_11=[$28])
                           +- LogicalTableScan(table=[[default_catalog, default_database, store]])

== Optimized Physical Plan ==
Sort(orderBy=[EXPR$0 ASC])
+- Exchange(distribution=[single])
   +- HashAggregate(isMerge=[true], groupBy=[EXPR$0], select=[EXPR$0])
      +- Exchange(distribution=[hash[EXPR$0]])
         +- LocalHashAggregate(groupBy=[EXPR$0], select=[EXPR$0])
            +- Calc(select=[EXPR$0], where=[>(EXPR$0, 19)])
               +- HashAggregate(isMerge=[true], groupBy=[ws_sold_time_sk_node_9], select=[ws_sold_time_sk_node_9, Final_MIN(min$0) AS EXPR$0])
                  +- Exchange(distribution=[hash[ws_sold_time_sk_node_9]])
                     +- LocalHashAggregate(groupBy=[ws_sold_time_sk_node_9], select=[ws_sold_time_sk_node_9, Partial_MIN(ws_web_page_sk_node_9) AS min$0])
                        +- Calc(select=[oxhhL, ws_sold_time_sk_node_9, ws_ship_date_sk_node_9, ws_item_sk_node_9, ws_bill_customer_sk_node_9, ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk_node_9, ws_bill_addr_sk_node_9, ws_ship_customer_sk_node_9, ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk_node_9, ws_ship_addr_sk_node_9, ws_web_page_sk_node_9, ws_web_site_sk_node_9, ws_ship_mode_sk_node_9, ws_warehouse_sk_node_9, ws_promo_sk_node_9, ws_order_number_node_9, ws_quantity_node_9, ws_wholesale_cost_node_9, ws_list_price_node_9, ws_sales_price_node_9, ws_ext_discount_amt_node_9, ws_ext_sales_price_node_9, ws_ext_wholesale_cost_node_9, ws_ext_list_price_node_9, ws_ext_tax_node_9, ws_coupon_amt_node_9, ws_ext_ship_cost_node_9, ws_net_paid_node_9, ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax_node_9, ws_net_profit_node_9, s_gmt_offset_node_11])
                           +- HashJoin(joinType=[InnerJoin], where=[=(s_gmt_offset_node_110, ws_ext_tax_node_9)], select=[oxhhL, ws_sold_time_sk_node_9, ws_ship_date_sk_node_9, ws_item_sk_node_9, ws_bill_customer_sk_node_9, ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk_node_9, ws_bill_addr_sk_node_9, ws_ship_customer_sk_node_9, ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk_node_9, ws_ship_addr_sk_node_9, ws_web_page_sk_node_9, ws_web_site_sk_node_9, ws_ship_mode_sk_node_9, ws_warehouse_sk_node_9, ws_promo_sk_node_9, ws_order_number_node_9, ws_quantity_node_9, ws_wholesale_cost_node_9, ws_list_price_node_9, ws_sales_price_node_9, ws_ext_discount_amt_node_9, ws_ext_sales_price_node_9, ws_ext_wholesale_cost_node_9, ws_ext_list_price_node_9, ws_ext_tax_node_9, ws_coupon_amt_node_9, ws_ext_ship_cost_node_9, ws_net_paid_node_9, ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax_node_9, ws_net_profit_node_9, s_gmt_offset_node_11, s_gmt_offset_node_110], isBroadcast=[true], build=[right])
                              :- Sort(orderBy=[ws_ship_cdemo_sk_node_9 ASC])
                              :  +- Exchange(distribution=[single])
                              :     +- Calc(select=[ws_sold_date_sk AS oxhhL, ws_sold_time_sk AS ws_sold_time_sk_node_9, ws_ship_date_sk AS ws_ship_date_sk_node_9, ws_item_sk AS ws_item_sk_node_9, ws_bill_customer_sk AS ws_bill_customer_sk_node_9, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_9, ws_bill_addr_sk AS ws_bill_addr_sk_node_9, ws_ship_customer_sk AS ws_ship_customer_sk_node_9, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_9, ws_ship_addr_sk AS ws_ship_addr_sk_node_9, ws_web_page_sk AS ws_web_page_sk_node_9, ws_web_site_sk AS ws_web_site_sk_node_9, ws_ship_mode_sk AS ws_ship_mode_sk_node_9, ws_warehouse_sk AS ws_warehouse_sk_node_9, ws_promo_sk AS ws_promo_sk_node_9, ws_order_number AS ws_order_number_node_9, ws_quantity AS ws_quantity_node_9, ws_wholesale_cost AS ws_wholesale_cost_node_9, ws_list_price AS ws_list_price_node_9, ws_sales_price AS ws_sales_price_node_9, ws_ext_discount_amt AS ws_ext_discount_amt_node_9, ws_ext_sales_price AS ws_ext_sales_price_node_9, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_9, ws_ext_list_price AS ws_ext_list_price_node_9, ws_ext_tax AS ws_ext_tax_node_9, ws_coupon_amt AS ws_coupon_amt_node_9, ws_ext_ship_cost AS ws_ext_ship_cost_node_9, ws_net_paid AS ws_net_paid_node_9, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_9, ws_net_profit AS ws_net_profit_node_9])
                              :        +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                              +- Exchange(distribution=[broadcast])
                                 +- Calc(select=[EXPR$0 AS s_gmt_offset_node_11, CAST(EXPR$0 AS DECIMAL(7, 2)) AS s_gmt_offset_node_110])
                                    +- HashAggregate(isMerge=[true], groupBy=[sr_return_quantity], select=[sr_return_quantity, Final_MAX(max$0) AS EXPR$0])
                                       +- Exchange(distribution=[hash[sr_return_quantity]])
                                          +- LocalHashAggregate(groupBy=[sr_return_quantity], select=[sr_return_quantity, Partial_MAX(s_gmt_offset) AS max$0])
                                             +- HashJoin(joinType=[InnerJoin], where=[=(s_closed_date_sk, sr_customer_sk)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], isBroadcast=[true], build=[right])
                                                :- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                                                +- Exchange(distribution=[broadcast])
                                                   +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])

== Optimized Execution Plan ==
Sort(orderBy=[EXPR$0 ASC])
+- Exchange(distribution=[single])
   +- HashAggregate(isMerge=[true], groupBy=[EXPR$0], select=[EXPR$0])
      +- Exchange(distribution=[hash[EXPR$0]])
         +- LocalHashAggregate(groupBy=[EXPR$0], select=[EXPR$0])
            +- Calc(select=[EXPR$0], where=[(EXPR$0 > 19)])
               +- HashAggregate(isMerge=[true], groupBy=[ws_sold_time_sk_node_9], select=[ws_sold_time_sk_node_9, Final_MIN(min$0) AS EXPR$0])
                  +- Exchange(distribution=[hash[ws_sold_time_sk_node_9]])
                     +- LocalHashAggregate(groupBy=[ws_sold_time_sk_node_9], select=[ws_sold_time_sk_node_9, Partial_MIN(ws_web_page_sk_node_9) AS min$0])
                        +- Calc(select=[oxhhL, ws_sold_time_sk_node_9, ws_ship_date_sk_node_9, ws_item_sk_node_9, ws_bill_customer_sk_node_9, ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk_node_9, ws_bill_addr_sk_node_9, ws_ship_customer_sk_node_9, ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk_node_9, ws_ship_addr_sk_node_9, ws_web_page_sk_node_9, ws_web_site_sk_node_9, ws_ship_mode_sk_node_9, ws_warehouse_sk_node_9, ws_promo_sk_node_9, ws_order_number_node_9, ws_quantity_node_9, ws_wholesale_cost_node_9, ws_list_price_node_9, ws_sales_price_node_9, ws_ext_discount_amt_node_9, ws_ext_sales_price_node_9, ws_ext_wholesale_cost_node_9, ws_ext_list_price_node_9, ws_ext_tax_node_9, ws_coupon_amt_node_9, ws_ext_ship_cost_node_9, ws_net_paid_node_9, ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax_node_9, ws_net_profit_node_9, s_gmt_offset_node_11])
                           +- HashJoin(joinType=[InnerJoin], where=[(s_gmt_offset_node_110 = ws_ext_tax_node_9)], select=[oxhhL, ws_sold_time_sk_node_9, ws_ship_date_sk_node_9, ws_item_sk_node_9, ws_bill_customer_sk_node_9, ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk_node_9, ws_bill_addr_sk_node_9, ws_ship_customer_sk_node_9, ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk_node_9, ws_ship_addr_sk_node_9, ws_web_page_sk_node_9, ws_web_site_sk_node_9, ws_ship_mode_sk_node_9, ws_warehouse_sk_node_9, ws_promo_sk_node_9, ws_order_number_node_9, ws_quantity_node_9, ws_wholesale_cost_node_9, ws_list_price_node_9, ws_sales_price_node_9, ws_ext_discount_amt_node_9, ws_ext_sales_price_node_9, ws_ext_wholesale_cost_node_9, ws_ext_list_price_node_9, ws_ext_tax_node_9, ws_coupon_amt_node_9, ws_ext_ship_cost_node_9, ws_net_paid_node_9, ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax_node_9, ws_net_profit_node_9, s_gmt_offset_node_11, s_gmt_offset_node_110], isBroadcast=[true], build=[right])
                              :- Sort(orderBy=[ws_ship_cdemo_sk_node_9 ASC])
                              :  +- Exchange(distribution=[single])
                              :     +- Calc(select=[ws_sold_date_sk AS oxhhL, ws_sold_time_sk AS ws_sold_time_sk_node_9, ws_ship_date_sk AS ws_ship_date_sk_node_9, ws_item_sk AS ws_item_sk_node_9, ws_bill_customer_sk AS ws_bill_customer_sk_node_9, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_9, ws_bill_addr_sk AS ws_bill_addr_sk_node_9, ws_ship_customer_sk AS ws_ship_customer_sk_node_9, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_9, ws_ship_addr_sk AS ws_ship_addr_sk_node_9, ws_web_page_sk AS ws_web_page_sk_node_9, ws_web_site_sk AS ws_web_site_sk_node_9, ws_ship_mode_sk AS ws_ship_mode_sk_node_9, ws_warehouse_sk AS ws_warehouse_sk_node_9, ws_promo_sk AS ws_promo_sk_node_9, ws_order_number AS ws_order_number_node_9, ws_quantity AS ws_quantity_node_9, ws_wholesale_cost AS ws_wholesale_cost_node_9, ws_list_price AS ws_list_price_node_9, ws_sales_price AS ws_sales_price_node_9, ws_ext_discount_amt AS ws_ext_discount_amt_node_9, ws_ext_sales_price AS ws_ext_sales_price_node_9, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_9, ws_ext_list_price AS ws_ext_list_price_node_9, ws_ext_tax AS ws_ext_tax_node_9, ws_coupon_amt AS ws_coupon_amt_node_9, ws_ext_ship_cost AS ws_ext_ship_cost_node_9, ws_net_paid AS ws_net_paid_node_9, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_9, ws_net_profit AS ws_net_profit_node_9])
                              :        +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                              +- Exchange(distribution=[broadcast])
                                 +- Calc(select=[EXPR$0 AS s_gmt_offset_node_11, CAST(EXPR$0 AS DECIMAL(7, 2)) AS s_gmt_offset_node_110])
                                    +- HashAggregate(isMerge=[true], groupBy=[sr_return_quantity], select=[sr_return_quantity, Final_MAX(max$0) AS EXPR$0])
                                       +- Exchange(distribution=[hash[sr_return_quantity]])
                                          +- LocalHashAggregate(groupBy=[sr_return_quantity], select=[sr_return_quantity, Partial_MAX(s_gmt_offset) AS max$0])
                                             +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(s_closed_date_sk = sr_customer_sk)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])\
+- [#2] Exchange(distribution=[broadcast])\
])
                                                :- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                                                +- Exchange(distribution=[broadcast])
                                                   +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o86202941.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#173549711:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[9](input=RelSubset#173549709,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[9]), rel#173549708:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[9](input=RelSubset#173549707,groupBy=ws_sold_time_sk, ws_ext_tax,select=ws_sold_time_sk, ws_ext_tax, Partial_MIN(ws_web_page_sk) AS min$0)]
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