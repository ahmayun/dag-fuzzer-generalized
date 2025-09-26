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

autonode_13 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_12 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_11 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_14 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_9 = autonode_12.order_by(col('cs_ship_mode_sk_node_12'))
autonode_8 = autonode_11.distinct()
autonode_10 = autonode_13.join(autonode_14, col('w_gmt_offset_node_13') == col('ws_ext_discount_amt_node_14'))
autonode_6 = autonode_8.alias('YOyHg')
autonode_7 = autonode_9.join(autonode_10, col('ws_ship_hdemo_sk_node_14') == col('cs_sold_date_sk_node_12'))
autonode_4 = autonode_6.add_columns(lit("hello"))
autonode_5 = autonode_7.group_by(col('cs_bill_addr_sk_node_12')).select(col('cs_bill_addr_sk_node_12').min.alias('cs_bill_addr_sk_node_12'))
autonode_2 = autonode_4.distinct()
autonode_3 = autonode_5.group_by(col('cs_bill_addr_sk_node_12')).select(col('cs_bill_addr_sk_node_12').min.alias('cs_bill_addr_sk_node_12'))
autonode_1 = autonode_2.join(autonode_3, col('cs_bill_addr_sk_node_12') == col('sr_cdemo_sk_node_11'))
sink = autonode_1.filter(preloaded_udf_boolean(col('sr_return_amt_inc_tax_node_11')))
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
LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($13)])
+- LogicalJoin(condition=[=($21, $4)], joinType=[inner])
   :- LogicalProject(YOyHg=[AS($0, _UTF-16LE'YOyHg')], sr_return_time_sk_node_11=[$1], sr_item_sk_node_11=[$2], sr_customer_sk_node_11=[$3], sr_cdemo_sk_node_11=[$4], sr_hdemo_sk_node_11=[$5], sr_addr_sk_node_11=[$6], sr_store_sk_node_11=[$7], sr_reason_sk_node_11=[$8], sr_ticket_number_node_11=[$9], sr_return_quantity_node_11=[$10], sr_return_amt_node_11=[$11], sr_return_tax_node_11=[$12], sr_return_amt_inc_tax_node_11=[$13], sr_fee_node_11=[$14], sr_return_ship_cost_node_11=[$15], sr_refunded_cash_node_11=[$16], sr_reversed_charge_node_11=[$17], sr_store_credit_node_11=[$18], sr_net_loss_node_11=[$19], _c20=[_UTF-16LE'hello'])
   :  +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}])
   :     +- LogicalProject(sr_returned_date_sk_node_11=[$0], sr_return_time_sk_node_11=[$1], sr_item_sk_node_11=[$2], sr_customer_sk_node_11=[$3], sr_cdemo_sk_node_11=[$4], sr_hdemo_sk_node_11=[$5], sr_addr_sk_node_11=[$6], sr_store_sk_node_11=[$7], sr_reason_sk_node_11=[$8], sr_ticket_number_node_11=[$9], sr_return_quantity_node_11=[$10], sr_return_amt_node_11=[$11], sr_return_tax_node_11=[$12], sr_return_amt_inc_tax_node_11=[$13], sr_fee_node_11=[$14], sr_return_ship_cost_node_11=[$15], sr_refunded_cash_node_11=[$16], sr_reversed_charge_node_11=[$17], sr_store_credit_node_11=[$18], sr_net_loss_node_11=[$19])
   :        +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
   +- LogicalProject(cs_bill_addr_sk_node_12=[$1])
      +- LogicalAggregate(group=[{0}], EXPR$0=[MIN($0)])
         +- LogicalProject(cs_bill_addr_sk_node_12=[$1])
            +- LogicalAggregate(group=[{6}], EXPR$0=[MIN($6)])
               +- LogicalJoin(condition=[=($58, $0)], joinType=[inner])
                  :- LogicalSort(sort0=[$13], dir0=[ASC])
                  :  +- LogicalProject(cs_sold_date_sk_node_12=[$0], cs_sold_time_sk_node_12=[$1], cs_ship_date_sk_node_12=[$2], cs_bill_customer_sk_node_12=[$3], cs_bill_cdemo_sk_node_12=[$4], cs_bill_hdemo_sk_node_12=[$5], cs_bill_addr_sk_node_12=[$6], cs_ship_customer_sk_node_12=[$7], cs_ship_cdemo_sk_node_12=[$8], cs_ship_hdemo_sk_node_12=[$9], cs_ship_addr_sk_node_12=[$10], cs_call_center_sk_node_12=[$11], cs_catalog_page_sk_node_12=[$12], cs_ship_mode_sk_node_12=[$13], cs_warehouse_sk_node_12=[$14], cs_item_sk_node_12=[$15], cs_promo_sk_node_12=[$16], cs_order_number_node_12=[$17], cs_quantity_node_12=[$18], cs_wholesale_cost_node_12=[$19], cs_list_price_node_12=[$20], cs_sales_price_node_12=[$21], cs_ext_discount_amt_node_12=[$22], cs_ext_sales_price_node_12=[$23], cs_ext_wholesale_cost_node_12=[$24], cs_ext_list_price_node_12=[$25], cs_ext_tax_node_12=[$26], cs_coupon_amt_node_12=[$27], cs_ext_ship_cost_node_12=[$28], cs_net_paid_node_12=[$29], cs_net_paid_inc_tax_node_12=[$30], cs_net_paid_inc_ship_node_12=[$31], cs_net_paid_inc_ship_tax_node_12=[$32], cs_net_profit_node_12=[$33])
                  :     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
                  +- LogicalJoin(condition=[=($13, $36)], joinType=[inner])
                     :- LogicalProject(w_warehouse_sk_node_13=[$0], w_warehouse_id_node_13=[$1], w_warehouse_name_node_13=[$2], w_warehouse_sq_ft_node_13=[$3], w_street_number_node_13=[$4], w_street_name_node_13=[$5], w_street_type_node_13=[$6], w_suite_number_node_13=[$7], w_city_node_13=[$8], w_county_node_13=[$9], w_state_node_13=[$10], w_zip_node_13=[$11], w_country_node_13=[$12], w_gmt_offset_node_13=[$13])
                     :  +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])
                     +- LogicalProject(ws_sold_date_sk_node_14=[$0], ws_sold_time_sk_node_14=[$1], ws_ship_date_sk_node_14=[$2], ws_item_sk_node_14=[$3], ws_bill_customer_sk_node_14=[$4], ws_bill_cdemo_sk_node_14=[$5], ws_bill_hdemo_sk_node_14=[$6], ws_bill_addr_sk_node_14=[$7], ws_ship_customer_sk_node_14=[$8], ws_ship_cdemo_sk_node_14=[$9], ws_ship_hdemo_sk_node_14=[$10], ws_ship_addr_sk_node_14=[$11], ws_web_page_sk_node_14=[$12], ws_web_site_sk_node_14=[$13], ws_ship_mode_sk_node_14=[$14], ws_warehouse_sk_node_14=[$15], ws_promo_sk_node_14=[$16], ws_order_number_node_14=[$17], ws_quantity_node_14=[$18], ws_wholesale_cost_node_14=[$19], ws_list_price_node_14=[$20], ws_sales_price_node_14=[$21], ws_ext_discount_amt_node_14=[$22], ws_ext_sales_price_node_14=[$23], ws_ext_wholesale_cost_node_14=[$24], ws_ext_list_price_node_14=[$25], ws_ext_tax_node_14=[$26], ws_coupon_amt_node_14=[$27], ws_ext_ship_cost_node_14=[$28], ws_net_paid_node_14=[$29], ws_net_paid_inc_tax_node_14=[$30], ws_net_paid_inc_ship_node_14=[$31], ws_net_paid_inc_ship_tax_node_14=[$32], ws_net_profit_node_14=[$33])
                        +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])

== Optimized Physical Plan ==
HashJoin(joinType=[InnerJoin], where=[=(cs_bill_addr_sk_node_12, sr_cdemo_sk_node_11)], select=[YOyHg, sr_return_time_sk_node_11, sr_item_sk_node_11, sr_customer_sk_node_11, sr_cdemo_sk_node_11, sr_hdemo_sk_node_11, sr_addr_sk_node_11, sr_store_sk_node_11, sr_reason_sk_node_11, sr_ticket_number_node_11, sr_return_quantity_node_11, sr_return_amt_node_11, sr_return_tax_node_11, sr_return_amt_inc_tax_node_11, sr_fee_node_11, sr_return_ship_cost_node_11, sr_refunded_cash_node_11, sr_reversed_charge_node_11, sr_store_credit_node_11, sr_net_loss_node_11, _c20, cs_bill_addr_sk_node_12], isBroadcast=[true], build=[right])
:- Calc(select=[sr_returned_date_sk AS YOyHg, sr_return_time_sk AS sr_return_time_sk_node_11, sr_item_sk AS sr_item_sk_node_11, sr_customer_sk AS sr_customer_sk_node_11, sr_cdemo_sk AS sr_cdemo_sk_node_11, sr_hdemo_sk AS sr_hdemo_sk_node_11, sr_addr_sk AS sr_addr_sk_node_11, sr_store_sk AS sr_store_sk_node_11, sr_reason_sk AS sr_reason_sk_node_11, sr_ticket_number AS sr_ticket_number_node_11, sr_return_quantity AS sr_return_quantity_node_11, sr_return_amt AS sr_return_amt_node_11, sr_return_tax AS sr_return_tax_node_11, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_11, sr_fee AS sr_fee_node_11, sr_return_ship_cost AS sr_return_ship_cost_node_11, sr_refunded_cash AS sr_refunded_cash_node_11, sr_reversed_charge AS sr_reversed_charge_node_11, sr_store_credit AS sr_store_credit_node_11, sr_net_loss AS sr_net_loss_node_11, 'hello' AS _c20])
:  +- HashAggregate(isMerge=[false], groupBy=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
:     +- Exchange(distribution=[hash[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss]])
:        +- Calc(select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], where=[f0])
:           +- PythonCalc(select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(sr_return_amt_inc_tax) AS f0])
:              +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
+- Exchange(distribution=[broadcast])
   +- Calc(select=[EXPR$0 AS cs_bill_addr_sk_node_12])
      +- HashAggregate(isMerge=[true], groupBy=[cs_bill_addr_sk_node_12], select=[cs_bill_addr_sk_node_12, Final_MIN(min$0) AS EXPR$0])
         +- Exchange(distribution=[hash[cs_bill_addr_sk_node_12]])
            +- LocalHashAggregate(groupBy=[cs_bill_addr_sk_node_12], select=[cs_bill_addr_sk_node_12, Partial_MIN(cs_bill_addr_sk_node_12) AS min$0])
               +- Calc(select=[EXPR$0 AS cs_bill_addr_sk_node_12])
                  +- HashAggregate(isMerge=[true], groupBy=[cs_bill_addr_sk], select=[cs_bill_addr_sk, Final_MIN(min$0) AS EXPR$0])
                     +- Exchange(distribution=[hash[cs_bill_addr_sk]])
                        +- LocalHashAggregate(groupBy=[cs_bill_addr_sk], select=[cs_bill_addr_sk, Partial_MIN(cs_bill_addr_sk) AS min$0])
                           +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ws_ship_hdemo_sk_node_14, cs_sold_date_sk)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, w_warehouse_sk_node_13, w_warehouse_id_node_13, w_warehouse_name_node_13, w_warehouse_sq_ft_node_13, w_street_number_node_13, w_street_name_node_13, w_street_type_node_13, w_suite_number_node_13, w_city_node_13, w_county_node_13, w_state_node_13, w_zip_node_13, w_country_node_13, w_gmt_offset_node_13, ws_sold_date_sk_node_14, ws_sold_time_sk_node_14, ws_ship_date_sk_node_14, ws_item_sk_node_14, ws_bill_customer_sk_node_14, ws_bill_cdemo_sk_node_14, ws_bill_hdemo_sk_node_14, ws_bill_addr_sk_node_14, ws_ship_customer_sk_node_14, ws_ship_cdemo_sk_node_14, ws_ship_hdemo_sk_node_14, ws_ship_addr_sk_node_14, ws_web_page_sk_node_14, ws_web_site_sk_node_14, ws_ship_mode_sk_node_14, ws_warehouse_sk_node_14, ws_promo_sk_node_14, ws_order_number_node_14, ws_quantity_node_14, ws_wholesale_cost_node_14, ws_list_price_node_14, ws_sales_price_node_14, ws_ext_discount_amt_node_14, ws_ext_sales_price_node_14, ws_ext_wholesale_cost_node_14, ws_ext_list_price_node_14, ws_ext_tax_node_14, ws_coupon_amt_node_14, ws_ext_ship_cost_node_14, ws_net_paid_node_14, ws_net_paid_inc_tax_node_14, ws_net_paid_inc_ship_node_14, ws_net_paid_inc_ship_tax_node_14, ws_net_profit_node_14], build=[right])
                              :- Sort(orderBy=[cs_ship_mode_sk ASC])
                              :  +- Exchange(distribution=[single])
                              :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
                              +- Exchange(distribution=[broadcast])
                                 +- Calc(select=[w_warehouse_sk_node_13, w_warehouse_id_node_13, w_warehouse_name_node_13, w_warehouse_sq_ft_node_13, w_street_number_node_13, w_street_name_node_13, w_street_type_node_13, w_suite_number_node_13, w_city_node_13, w_county_node_13, w_state_node_13, w_zip_node_13, w_country_node_13, w_gmt_offset_node_13, ws_sold_date_sk AS ws_sold_date_sk_node_14, ws_sold_time_sk AS ws_sold_time_sk_node_14, ws_ship_date_sk AS ws_ship_date_sk_node_14, ws_item_sk AS ws_item_sk_node_14, ws_bill_customer_sk AS ws_bill_customer_sk_node_14, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_14, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_14, ws_bill_addr_sk AS ws_bill_addr_sk_node_14, ws_ship_customer_sk AS ws_ship_customer_sk_node_14, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_14, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_14, ws_ship_addr_sk AS ws_ship_addr_sk_node_14, ws_web_page_sk AS ws_web_page_sk_node_14, ws_web_site_sk AS ws_web_site_sk_node_14, ws_ship_mode_sk AS ws_ship_mode_sk_node_14, ws_warehouse_sk AS ws_warehouse_sk_node_14, ws_promo_sk AS ws_promo_sk_node_14, ws_order_number AS ws_order_number_node_14, ws_quantity AS ws_quantity_node_14, ws_wholesale_cost AS ws_wholesale_cost_node_14, ws_list_price AS ws_list_price_node_14, ws_sales_price AS ws_sales_price_node_14, ws_ext_discount_amt AS ws_ext_discount_amt_node_14, ws_ext_sales_price AS ws_ext_sales_price_node_14, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_14, ws_ext_list_price AS ws_ext_list_price_node_14, ws_ext_tax AS ws_ext_tax_node_14, ws_coupon_amt AS ws_coupon_amt_node_14, ws_ext_ship_cost AS ws_ext_ship_cost_node_14, ws_net_paid AS ws_net_paid_node_14, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_14, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_14, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_14, ws_net_profit AS ws_net_profit_node_14])
                                    +- NestedLoopJoin(joinType=[InnerJoin], where=[=(w_gmt_offset_node_130, ws_ext_discount_amt)], select=[w_warehouse_sk_node_13, w_warehouse_id_node_13, w_warehouse_name_node_13, w_warehouse_sq_ft_node_13, w_street_number_node_13, w_street_name_node_13, w_street_type_node_13, w_suite_number_node_13, w_city_node_13, w_county_node_13, w_state_node_13, w_zip_node_13, w_country_node_13, w_gmt_offset_node_13, w_gmt_offset_node_130, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
                                       :- Exchange(distribution=[broadcast])
                                       :  +- Calc(select=[w_warehouse_sk AS w_warehouse_sk_node_13, w_warehouse_id AS w_warehouse_id_node_13, w_warehouse_name AS w_warehouse_name_node_13, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_13, w_street_number AS w_street_number_node_13, w_street_name AS w_street_name_node_13, w_street_type AS w_street_type_node_13, w_suite_number AS w_suite_number_node_13, w_city AS w_city_node_13, w_county AS w_county_node_13, w_state AS w_state_node_13, w_zip AS w_zip_node_13, w_country AS w_country_node_13, w_gmt_offset AS w_gmt_offset_node_13, CAST(w_gmt_offset AS DECIMAL(7, 2)) AS w_gmt_offset_node_130])
                                       :     +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
                                       +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])

== Optimized Execution Plan ==
MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(cs_bill_addr_sk_node_12 = sr_cdemo_sk_node_11)], select=[YOyHg, sr_return_time_sk_node_11, sr_item_sk_node_11, sr_customer_sk_node_11, sr_cdemo_sk_node_11, sr_hdemo_sk_node_11, sr_addr_sk_node_11, sr_store_sk_node_11, sr_reason_sk_node_11, sr_ticket_number_node_11, sr_return_quantity_node_11, sr_return_amt_node_11, sr_return_tax_node_11, sr_return_amt_inc_tax_node_11, sr_fee_node_11, sr_return_ship_cost_node_11, sr_refunded_cash_node_11, sr_reversed_charge_node_11, sr_store_credit_node_11, sr_net_loss_node_11, _c20, cs_bill_addr_sk_node_12], isBroadcast=[true], build=[right])\
:- Calc(select=[sr_returned_date_sk AS YOyHg, sr_return_time_sk AS sr_return_time_sk_node_11, sr_item_sk AS sr_item_sk_node_11, sr_customer_sk AS sr_customer_sk_node_11, sr_cdemo_sk AS sr_cdemo_sk_node_11, sr_hdemo_sk AS sr_hdemo_sk_node_11, sr_addr_sk AS sr_addr_sk_node_11, sr_store_sk AS sr_store_sk_node_11, sr_reason_sk AS sr_reason_sk_node_11, sr_ticket_number AS sr_ticket_number_node_11, sr_return_quantity AS sr_return_quantity_node_11, sr_return_amt AS sr_return_amt_node_11, sr_return_tax AS sr_return_tax_node_11, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_11, sr_fee AS sr_fee_node_11, sr_return_ship_cost AS sr_return_ship_cost_node_11, sr_refunded_cash AS sr_refunded_cash_node_11, sr_reversed_charge AS sr_reversed_charge_node_11, sr_store_credit AS sr_store_credit_node_11, sr_net_loss AS sr_net_loss_node_11, 'hello' AS _c20])\
:  +- HashAggregate(isMerge=[false], groupBy=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])\
:     +- [#2] Exchange(distribution=[hash[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss]])\
+- [#1] Exchange(distribution=[broadcast])\
])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[EXPR$0 AS cs_bill_addr_sk_node_12])
:     +- HashAggregate(isMerge=[true], groupBy=[cs_bill_addr_sk_node_12], select=[cs_bill_addr_sk_node_12, Final_MIN(min$0) AS EXPR$0])
:        +- Exchange(distribution=[hash[cs_bill_addr_sk_node_12]])
:           +- LocalHashAggregate(groupBy=[cs_bill_addr_sk_node_12], select=[cs_bill_addr_sk_node_12, Partial_MIN(cs_bill_addr_sk_node_12) AS min$0])
:              +- Calc(select=[EXPR$0 AS cs_bill_addr_sk_node_12])
:                 +- HashAggregate(isMerge=[true], groupBy=[cs_bill_addr_sk], select=[cs_bill_addr_sk, Final_MIN(min$0) AS EXPR$0])
:                    +- Exchange(distribution=[hash[cs_bill_addr_sk]])
:                       +- LocalHashAggregate(groupBy=[cs_bill_addr_sk], select=[cs_bill_addr_sk, Partial_MIN(cs_bill_addr_sk) AS min$0])
:                          +- NestedLoopJoin(joinType=[InnerJoin], where=[(ws_ship_hdemo_sk_node_14 = cs_sold_date_sk)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, w_warehouse_sk_node_13, w_warehouse_id_node_13, w_warehouse_name_node_13, w_warehouse_sq_ft_node_13, w_street_number_node_13, w_street_name_node_13, w_street_type_node_13, w_suite_number_node_13, w_city_node_13, w_county_node_13, w_state_node_13, w_zip_node_13, w_country_node_13, w_gmt_offset_node_13, ws_sold_date_sk_node_14, ws_sold_time_sk_node_14, ws_ship_date_sk_node_14, ws_item_sk_node_14, ws_bill_customer_sk_node_14, ws_bill_cdemo_sk_node_14, ws_bill_hdemo_sk_node_14, ws_bill_addr_sk_node_14, ws_ship_customer_sk_node_14, ws_ship_cdemo_sk_node_14, ws_ship_hdemo_sk_node_14, ws_ship_addr_sk_node_14, ws_web_page_sk_node_14, ws_web_site_sk_node_14, ws_ship_mode_sk_node_14, ws_warehouse_sk_node_14, ws_promo_sk_node_14, ws_order_number_node_14, ws_quantity_node_14, ws_wholesale_cost_node_14, ws_list_price_node_14, ws_sales_price_node_14, ws_ext_discount_amt_node_14, ws_ext_sales_price_node_14, ws_ext_wholesale_cost_node_14, ws_ext_list_price_node_14, ws_ext_tax_node_14, ws_coupon_amt_node_14, ws_ext_ship_cost_node_14, ws_net_paid_node_14, ws_net_paid_inc_tax_node_14, ws_net_paid_inc_ship_node_14, ws_net_paid_inc_ship_tax_node_14, ws_net_profit_node_14], build=[right])
:                             :- Sort(orderBy=[cs_ship_mode_sk ASC])
:                             :  +- Exchange(distribution=[single])
:                             :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
:                             +- Exchange(distribution=[broadcast])
:                                +- Calc(select=[w_warehouse_sk_node_13, w_warehouse_id_node_13, w_warehouse_name_node_13, w_warehouse_sq_ft_node_13, w_street_number_node_13, w_street_name_node_13, w_street_type_node_13, w_suite_number_node_13, w_city_node_13, w_county_node_13, w_state_node_13, w_zip_node_13, w_country_node_13, w_gmt_offset_node_13, ws_sold_date_sk AS ws_sold_date_sk_node_14, ws_sold_time_sk AS ws_sold_time_sk_node_14, ws_ship_date_sk AS ws_ship_date_sk_node_14, ws_item_sk AS ws_item_sk_node_14, ws_bill_customer_sk AS ws_bill_customer_sk_node_14, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_14, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_14, ws_bill_addr_sk AS ws_bill_addr_sk_node_14, ws_ship_customer_sk AS ws_ship_customer_sk_node_14, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_14, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_14, ws_ship_addr_sk AS ws_ship_addr_sk_node_14, ws_web_page_sk AS ws_web_page_sk_node_14, ws_web_site_sk AS ws_web_site_sk_node_14, ws_ship_mode_sk AS ws_ship_mode_sk_node_14, ws_warehouse_sk AS ws_warehouse_sk_node_14, ws_promo_sk AS ws_promo_sk_node_14, ws_order_number AS ws_order_number_node_14, ws_quantity AS ws_quantity_node_14, ws_wholesale_cost AS ws_wholesale_cost_node_14, ws_list_price AS ws_list_price_node_14, ws_sales_price AS ws_sales_price_node_14, ws_ext_discount_amt AS ws_ext_discount_amt_node_14, ws_ext_sales_price AS ws_ext_sales_price_node_14, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_14, ws_ext_list_price AS ws_ext_list_price_node_14, ws_ext_tax AS ws_ext_tax_node_14, ws_coupon_amt AS ws_coupon_amt_node_14, ws_ext_ship_cost AS ws_ext_ship_cost_node_14, ws_net_paid AS ws_net_paid_node_14, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_14, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_14, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_14, ws_net_profit AS ws_net_profit_node_14])
:                                   +- MultipleInput(readOrder=[0,1], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(w_gmt_offset_node_130 = ws_ext_discount_amt)], select=[w_warehouse_sk_node_13, w_warehouse_id_node_13, w_warehouse_name_node_13, w_warehouse_sq_ft_node_13, w_street_number_node_13, w_street_name_node_13, w_street_type_node_13, w_suite_number_node_13, w_city_node_13, w_county_node_13, w_state_node_13, w_zip_node_13, w_country_node_13, w_gmt_offset_node_13, w_gmt_offset_node_130, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])\
])
:                                      :- Exchange(distribution=[broadcast])
:                                      :  +- Calc(select=[w_warehouse_sk AS w_warehouse_sk_node_13, w_warehouse_id AS w_warehouse_id_node_13, w_warehouse_name AS w_warehouse_name_node_13, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_13, w_street_number AS w_street_number_node_13, w_street_name AS w_street_name_node_13, w_street_type AS w_street_type_node_13, w_suite_number AS w_suite_number_node_13, w_city AS w_city_node_13, w_county AS w_county_node_13, w_state AS w_state_node_13, w_zip AS w_zip_node_13, w_country AS w_country_node_13, w_gmt_offset AS w_gmt_offset_node_13, CAST(w_gmt_offset AS DECIMAL(7, 2)) AS w_gmt_offset_node_130])
:                                      :     +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
:                                      +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
+- Exchange(distribution=[hash[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss]])
   +- Calc(select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], where=[f0])
      +- PythonCalc(select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(sr_return_amt_inc_tax) AS f0])
         +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o68549682.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#137391775:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[13](input=RelSubset#137391773,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[13]), rel#137391772:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[13](input=RelSubset#137391771,groupBy=cs_sold_date_sk, cs_bill_addr_sk,select=cs_sold_date_sk, cs_bill_addr_sk, Partial_MIN(cs_bill_addr_sk) AS min$0)]
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