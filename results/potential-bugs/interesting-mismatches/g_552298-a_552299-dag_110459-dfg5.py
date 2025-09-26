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
    return values.sum()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_9 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_8 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_7 = autonode_10.distinct()
autonode_6 = autonode_9.order_by(col('ss_net_paid_node_9'))
autonode_5 = autonode_8.group_by(col('hd_dep_count_node_8')).select(col('hd_vehicle_count_node_8').max.alias('hd_vehicle_count_node_8'))
autonode_4 = autonode_7.alias('aFF91')
autonode_3 = autonode_5.join(autonode_6, col('ss_hdemo_sk_node_9') == col('hd_vehicle_count_node_8'))
autonode_2 = autonode_3.join(autonode_4, col('ss_net_paid_inc_tax_node_9') == col('cs_coupon_amt_node_10'))
autonode_1 = autonode_2.group_by(col('ss_item_sk_node_9')).select(col('cs_ship_mode_sk_node_10').min.alias('cs_ship_mode_sk_node_10'))
sink = autonode_1.alias('bW5Vg')
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
LogicalProject(bW5Vg=[AS($1, _UTF-16LE'bW5Vg')])
+- LogicalAggregate(group=[{3}], EXPR$0=[MIN($37)])
   +- LogicalJoin(condition=[=($22, $51)], joinType=[inner])
      :- LogicalJoin(condition=[=($6, $0)], joinType=[inner])
      :  :- LogicalProject(hd_vehicle_count_node_8=[$1])
      :  :  +- LogicalAggregate(group=[{0}], EXPR$0=[MAX($1)])
      :  :     +- LogicalProject(hd_dep_count_node_8=[$3], hd_vehicle_count_node_8=[$4])
      :  :        +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
      :  +- LogicalSort(sort0=[$20], dir0=[ASC])
      :     +- LogicalProject(ss_sold_date_sk_node_9=[$0], ss_sold_time_sk_node_9=[$1], ss_item_sk_node_9=[$2], ss_customer_sk_node_9=[$3], ss_cdemo_sk_node_9=[$4], ss_hdemo_sk_node_9=[$5], ss_addr_sk_node_9=[$6], ss_store_sk_node_9=[$7], ss_promo_sk_node_9=[$8], ss_ticket_number_node_9=[$9], ss_quantity_node_9=[$10], ss_wholesale_cost_node_9=[$11], ss_list_price_node_9=[$12], ss_sales_price_node_9=[$13], ss_ext_discount_amt_node_9=[$14], ss_ext_sales_price_node_9=[$15], ss_ext_wholesale_cost_node_9=[$16], ss_ext_list_price_node_9=[$17], ss_ext_tax_node_9=[$18], ss_coupon_amt_node_9=[$19], ss_net_paid_node_9=[$20], ss_net_paid_inc_tax_node_9=[$21], ss_net_profit_node_9=[$22])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalProject(aFF91=[AS($0, _UTF-16LE'aFF91')], cs_sold_time_sk_node_10=[$1], cs_ship_date_sk_node_10=[$2], cs_bill_customer_sk_node_10=[$3], cs_bill_cdemo_sk_node_10=[$4], cs_bill_hdemo_sk_node_10=[$5], cs_bill_addr_sk_node_10=[$6], cs_ship_customer_sk_node_10=[$7], cs_ship_cdemo_sk_node_10=[$8], cs_ship_hdemo_sk_node_10=[$9], cs_ship_addr_sk_node_10=[$10], cs_call_center_sk_node_10=[$11], cs_catalog_page_sk_node_10=[$12], cs_ship_mode_sk_node_10=[$13], cs_warehouse_sk_node_10=[$14], cs_item_sk_node_10=[$15], cs_promo_sk_node_10=[$16], cs_order_number_node_10=[$17], cs_quantity_node_10=[$18], cs_wholesale_cost_node_10=[$19], cs_list_price_node_10=[$20], cs_sales_price_node_10=[$21], cs_ext_discount_amt_node_10=[$22], cs_ext_sales_price_node_10=[$23], cs_ext_wholesale_cost_node_10=[$24], cs_ext_list_price_node_10=[$25], cs_ext_tax_node_10=[$26], cs_coupon_amt_node_10=[$27], cs_ext_ship_cost_node_10=[$28], cs_net_paid_node_10=[$29], cs_net_paid_inc_tax_node_10=[$30], cs_net_paid_inc_ship_node_10=[$31], cs_net_paid_inc_ship_tax_node_10=[$32], cs_net_profit_node_10=[$33])
         +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33}])
            +- LogicalProject(cs_sold_date_sk_node_10=[$0], cs_sold_time_sk_node_10=[$1], cs_ship_date_sk_node_10=[$2], cs_bill_customer_sk_node_10=[$3], cs_bill_cdemo_sk_node_10=[$4], cs_bill_hdemo_sk_node_10=[$5], cs_bill_addr_sk_node_10=[$6], cs_ship_customer_sk_node_10=[$7], cs_ship_cdemo_sk_node_10=[$8], cs_ship_hdemo_sk_node_10=[$9], cs_ship_addr_sk_node_10=[$10], cs_call_center_sk_node_10=[$11], cs_catalog_page_sk_node_10=[$12], cs_ship_mode_sk_node_10=[$13], cs_warehouse_sk_node_10=[$14], cs_item_sk_node_10=[$15], cs_promo_sk_node_10=[$16], cs_order_number_node_10=[$17], cs_quantity_node_10=[$18], cs_wholesale_cost_node_10=[$19], cs_list_price_node_10=[$20], cs_sales_price_node_10=[$21], cs_ext_discount_amt_node_10=[$22], cs_ext_sales_price_node_10=[$23], cs_ext_wholesale_cost_node_10=[$24], cs_ext_list_price_node_10=[$25], cs_ext_tax_node_10=[$26], cs_coupon_amt_node_10=[$27], cs_ext_ship_cost_node_10=[$28], cs_net_paid_node_10=[$29], cs_net_paid_inc_tax_node_10=[$30], cs_net_paid_inc_ship_node_10=[$31], cs_net_paid_inc_ship_tax_node_10=[$32], cs_net_profit_node_10=[$33])
               +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS bW5Vg])
+- HashAggregate(isMerge=[true], groupBy=[ss_item_sk], select=[ss_item_sk, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_item_sk]])
      +- LocalHashAggregate(groupBy=[ss_item_sk], select=[ss_item_sk, Partial_MIN(cs_ship_mode_sk_node_10) AS min$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(ss_net_paid_inc_tax, cs_coupon_amt_node_10)], select=[hd_vehicle_count_node_8, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, aFF91, cs_sold_time_sk_node_10, cs_ship_date_sk_node_10, cs_bill_customer_sk_node_10, cs_bill_cdemo_sk_node_10, cs_bill_hdemo_sk_node_10, cs_bill_addr_sk_node_10, cs_ship_customer_sk_node_10, cs_ship_cdemo_sk_node_10, cs_ship_hdemo_sk_node_10, cs_ship_addr_sk_node_10, cs_call_center_sk_node_10, cs_catalog_page_sk_node_10, cs_ship_mode_sk_node_10, cs_warehouse_sk_node_10, cs_item_sk_node_10, cs_promo_sk_node_10, cs_order_number_node_10, cs_quantity_node_10, cs_wholesale_cost_node_10, cs_list_price_node_10, cs_sales_price_node_10, cs_ext_discount_amt_node_10, cs_ext_sales_price_node_10, cs_ext_wholesale_cost_node_10, cs_ext_list_price_node_10, cs_ext_tax_node_10, cs_coupon_amt_node_10, cs_ext_ship_cost_node_10, cs_net_paid_node_10, cs_net_paid_inc_tax_node_10, cs_net_paid_inc_ship_node_10, cs_net_paid_inc_ship_tax_node_10, cs_net_profit_node_10], build=[left])
            :- Exchange(distribution=[hash[ss_net_paid_inc_tax]])
            :  +- HashJoin(joinType=[InnerJoin], where=[=(ss_hdemo_sk, hd_vehicle_count_node_8)], select=[hd_vehicle_count_node_8, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], isBroadcast=[true], build=[left])
            :     :- Exchange(distribution=[broadcast])
            :     :  +- Calc(select=[EXPR$0 AS hd_vehicle_count_node_8])
            :     :     +- HashAggregate(isMerge=[true], groupBy=[hd_dep_count], select=[hd_dep_count, Final_MAX(max$0) AS EXPR$0])
            :     :        +- Exchange(distribution=[hash[hd_dep_count]])
            :     :           +- LocalHashAggregate(groupBy=[hd_dep_count], select=[hd_dep_count, Partial_MAX(hd_vehicle_count) AS max$0])
            :     :              +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, project=[hd_dep_count, hd_vehicle_count], metadata=[]]], fields=[hd_dep_count, hd_vehicle_count])
            :     +- Sort(orderBy=[ss_net_paid ASC])
            :        +- Exchange(distribution=[single])
            :           +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[hash[cs_coupon_amt_node_10]])
               +- Calc(select=[cs_sold_date_sk AS aFF91, cs_sold_time_sk AS cs_sold_time_sk_node_10, cs_ship_date_sk AS cs_ship_date_sk_node_10, cs_bill_customer_sk AS cs_bill_customer_sk_node_10, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_10, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_10, cs_bill_addr_sk AS cs_bill_addr_sk_node_10, cs_ship_customer_sk AS cs_ship_customer_sk_node_10, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_10, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_10, cs_ship_addr_sk AS cs_ship_addr_sk_node_10, cs_call_center_sk AS cs_call_center_sk_node_10, cs_catalog_page_sk AS cs_catalog_page_sk_node_10, cs_ship_mode_sk AS cs_ship_mode_sk_node_10, cs_warehouse_sk AS cs_warehouse_sk_node_10, cs_item_sk AS cs_item_sk_node_10, cs_promo_sk AS cs_promo_sk_node_10, cs_order_number AS cs_order_number_node_10, cs_quantity AS cs_quantity_node_10, cs_wholesale_cost AS cs_wholesale_cost_node_10, cs_list_price AS cs_list_price_node_10, cs_sales_price AS cs_sales_price_node_10, cs_ext_discount_amt AS cs_ext_discount_amt_node_10, cs_ext_sales_price AS cs_ext_sales_price_node_10, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_10, cs_ext_list_price AS cs_ext_list_price_node_10, cs_ext_tax AS cs_ext_tax_node_10, cs_coupon_amt AS cs_coupon_amt_node_10, cs_ext_ship_cost AS cs_ext_ship_cost_node_10, cs_net_paid AS cs_net_paid_node_10, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_10, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_10, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_10, cs_net_profit AS cs_net_profit_node_10])
                  +- HashAggregate(isMerge=[false], groupBy=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
                     +- Exchange(distribution=[hash[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit]])
                        +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS bW5Vg])
+- HashAggregate(isMerge=[true], groupBy=[ss_item_sk], select=[ss_item_sk, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_item_sk]])
      +- LocalHashAggregate(groupBy=[ss_item_sk], select=[ss_item_sk, Partial_MIN(cs_ship_mode_sk_node_10) AS min$0])
         +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ss_net_paid_inc_tax = cs_coupon_amt_node_10)], select=[hd_vehicle_count_node_8, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, aFF91, cs_sold_time_sk_node_10, cs_ship_date_sk_node_10, cs_bill_customer_sk_node_10, cs_bill_cdemo_sk_node_10, cs_bill_hdemo_sk_node_10, cs_bill_addr_sk_node_10, cs_ship_customer_sk_node_10, cs_ship_cdemo_sk_node_10, cs_ship_hdemo_sk_node_10, cs_ship_addr_sk_node_10, cs_call_center_sk_node_10, cs_catalog_page_sk_node_10, cs_ship_mode_sk_node_10, cs_warehouse_sk_node_10, cs_item_sk_node_10, cs_promo_sk_node_10, cs_order_number_node_10, cs_quantity_node_10, cs_wholesale_cost_node_10, cs_list_price_node_10, cs_sales_price_node_10, cs_ext_discount_amt_node_10, cs_ext_sales_price_node_10, cs_ext_wholesale_cost_node_10, cs_ext_list_price_node_10, cs_ext_tax_node_10, cs_coupon_amt_node_10, cs_ext_ship_cost_node_10, cs_net_paid_node_10, cs_net_paid_inc_tax_node_10, cs_net_paid_inc_ship_node_10, cs_net_paid_inc_ship_tax_node_10, cs_net_profit_node_10], build=[left])
            :- Exchange(distribution=[hash[ss_net_paid_inc_tax]])
            :  +- HashJoin(joinType=[InnerJoin], where=[(ss_hdemo_sk = hd_vehicle_count_node_8)], select=[hd_vehicle_count_node_8, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], isBroadcast=[true], build=[left])
            :     :- Exchange(distribution=[broadcast])
            :     :  +- Calc(select=[EXPR$0 AS hd_vehicle_count_node_8])
            :     :     +- HashAggregate(isMerge=[true], groupBy=[hd_dep_count], select=[hd_dep_count, Final_MAX(max$0) AS EXPR$0])
            :     :        +- Exchange(distribution=[hash[hd_dep_count]])
            :     :           +- LocalHashAggregate(groupBy=[hd_dep_count], select=[hd_dep_count, Partial_MAX(hd_vehicle_count) AS max$0])
            :     :              +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, project=[hd_dep_count, hd_vehicle_count], metadata=[]]], fields=[hd_dep_count, hd_vehicle_count])
            :     +- Sort(orderBy=[ss_net_paid ASC])
            :        +- Exchange(distribution=[single])
            :           +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[hash[cs_coupon_amt_node_10]])
               +- Calc(select=[cs_sold_date_sk AS aFF91, cs_sold_time_sk AS cs_sold_time_sk_node_10, cs_ship_date_sk AS cs_ship_date_sk_node_10, cs_bill_customer_sk AS cs_bill_customer_sk_node_10, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_10, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_10, cs_bill_addr_sk AS cs_bill_addr_sk_node_10, cs_ship_customer_sk AS cs_ship_customer_sk_node_10, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_10, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_10, cs_ship_addr_sk AS cs_ship_addr_sk_node_10, cs_call_center_sk AS cs_call_center_sk_node_10, cs_catalog_page_sk AS cs_catalog_page_sk_node_10, cs_ship_mode_sk AS cs_ship_mode_sk_node_10, cs_warehouse_sk AS cs_warehouse_sk_node_10, cs_item_sk AS cs_item_sk_node_10, cs_promo_sk AS cs_promo_sk_node_10, cs_order_number AS cs_order_number_node_10, cs_quantity AS cs_quantity_node_10, cs_wholesale_cost AS cs_wholesale_cost_node_10, cs_list_price AS cs_list_price_node_10, cs_sales_price AS cs_sales_price_node_10, cs_ext_discount_amt AS cs_ext_discount_amt_node_10, cs_ext_sales_price AS cs_ext_sales_price_node_10, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_10, cs_ext_list_price AS cs_ext_list_price_node_10, cs_ext_tax AS cs_ext_tax_node_10, cs_coupon_amt AS cs_coupon_amt_node_10, cs_ext_ship_cost AS cs_ext_ship_cost_node_10, cs_net_paid AS cs_net_paid_node_10, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_10, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_10, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_10, cs_net_profit AS cs_net_profit_node_10])
                  +- HashAggregate(isMerge=[false], groupBy=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
                     +- Exchange(distribution=[hash[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit]])
                        +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o301064864.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#607334438:AbstractConverter.BATCH_PHYSICAL.hash[0, 1, 2]true.[20](input=RelSubset#607334436,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1, 2]true,sort=[20]), rel#607334435:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[20](input=RelSubset#607334434,groupBy=ss_item_sk, ss_hdemo_sk, ss_net_paid_inc_tax,select=ss_item_sk, ss_hdemo_sk, ss_net_paid_inc_tax)]
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