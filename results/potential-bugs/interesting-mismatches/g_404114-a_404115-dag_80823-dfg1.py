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
    return values.mean()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_14 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_15 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_16 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_10 = autonode_13.distinct()
autonode_11 = autonode_14.join(autonode_15, col('inv_warehouse_sk_node_14') == col('ss_hdemo_sk_node_15'))
autonode_12 = autonode_16.filter(col('ss_quantity_node_16') >= -45)
autonode_7 = autonode_10.alias('jygr5')
autonode_8 = autonode_11.group_by(col('ss_sold_date_sk_node_15')).select(col('ss_customer_sk_node_15').min.alias('ss_customer_sk_node_15'))
autonode_9 = autonode_12.select(col('ss_quantity_node_16'))
autonode_5 = autonode_7.join(autonode_8, col('ss_customer_sk_node_15') == col('cs_quantity_node_13'))
autonode_6 = autonode_9.group_by(col('ss_quantity_node_16')).select(col('ss_quantity_node_16').sum.alias('ss_quantity_node_16'))
autonode_3 = autonode_5.order_by(col('cs_ext_discount_amt_node_13'))
autonode_4 = autonode_6.add_columns(lit("hello"))
autonode_2 = autonode_3.join(autonode_4, col('ss_quantity_node_16') == col('cs_call_center_sk_node_13'))
autonode_1 = autonode_2.group_by(col('cs_catalog_page_sk_node_13')).select(col('cs_item_sk_node_13').min.alias('cs_item_sk_node_13'))
sink = autonode_1.filter(col('cs_item_sk_node_13') > -21)
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
LogicalFilter(condition=[>($0, -21)])
+- LogicalProject(cs_item_sk_node_13=[$1])
   +- LogicalAggregate(group=[{12}], EXPR$0=[MIN($15)])
      +- LogicalJoin(condition=[=($35, $11)], joinType=[inner])
         :- LogicalSort(sort0=[$22], dir0=[ASC])
         :  +- LogicalJoin(condition=[=($34, $18)], joinType=[inner])
         :     :- LogicalProject(jygr5=[AS($0, _UTF-16LE'jygr5')], cs_sold_time_sk_node_13=[$1], cs_ship_date_sk_node_13=[$2], cs_bill_customer_sk_node_13=[$3], cs_bill_cdemo_sk_node_13=[$4], cs_bill_hdemo_sk_node_13=[$5], cs_bill_addr_sk_node_13=[$6], cs_ship_customer_sk_node_13=[$7], cs_ship_cdemo_sk_node_13=[$8], cs_ship_hdemo_sk_node_13=[$9], cs_ship_addr_sk_node_13=[$10], cs_call_center_sk_node_13=[$11], cs_catalog_page_sk_node_13=[$12], cs_ship_mode_sk_node_13=[$13], cs_warehouse_sk_node_13=[$14], cs_item_sk_node_13=[$15], cs_promo_sk_node_13=[$16], cs_order_number_node_13=[$17], cs_quantity_node_13=[$18], cs_wholesale_cost_node_13=[$19], cs_list_price_node_13=[$20], cs_sales_price_node_13=[$21], cs_ext_discount_amt_node_13=[$22], cs_ext_sales_price_node_13=[$23], cs_ext_wholesale_cost_node_13=[$24], cs_ext_list_price_node_13=[$25], cs_ext_tax_node_13=[$26], cs_coupon_amt_node_13=[$27], cs_ext_ship_cost_node_13=[$28], cs_net_paid_node_13=[$29], cs_net_paid_inc_tax_node_13=[$30], cs_net_paid_inc_ship_node_13=[$31], cs_net_paid_inc_ship_tax_node_13=[$32], cs_net_profit_node_13=[$33])
         :     :  +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33}])
         :     :     +- LogicalProject(cs_sold_date_sk_node_13=[$0], cs_sold_time_sk_node_13=[$1], cs_ship_date_sk_node_13=[$2], cs_bill_customer_sk_node_13=[$3], cs_bill_cdemo_sk_node_13=[$4], cs_bill_hdemo_sk_node_13=[$5], cs_bill_addr_sk_node_13=[$6], cs_ship_customer_sk_node_13=[$7], cs_ship_cdemo_sk_node_13=[$8], cs_ship_hdemo_sk_node_13=[$9], cs_ship_addr_sk_node_13=[$10], cs_call_center_sk_node_13=[$11], cs_catalog_page_sk_node_13=[$12], cs_ship_mode_sk_node_13=[$13], cs_warehouse_sk_node_13=[$14], cs_item_sk_node_13=[$15], cs_promo_sk_node_13=[$16], cs_order_number_node_13=[$17], cs_quantity_node_13=[$18], cs_wholesale_cost_node_13=[$19], cs_list_price_node_13=[$20], cs_sales_price_node_13=[$21], cs_ext_discount_amt_node_13=[$22], cs_ext_sales_price_node_13=[$23], cs_ext_wholesale_cost_node_13=[$24], cs_ext_list_price_node_13=[$25], cs_ext_tax_node_13=[$26], cs_coupon_amt_node_13=[$27], cs_ext_ship_cost_node_13=[$28], cs_net_paid_node_13=[$29], cs_net_paid_inc_tax_node_13=[$30], cs_net_paid_inc_ship_node_13=[$31], cs_net_paid_inc_ship_tax_node_13=[$32], cs_net_profit_node_13=[$33])
         :     :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
         :     +- LogicalProject(ss_customer_sk_node_15=[$1])
         :        +- LogicalAggregate(group=[{4}], EXPR$0=[MIN($7)])
         :           +- LogicalJoin(condition=[=($2, $9)], joinType=[inner])
         :              :- LogicalProject(inv_date_sk_node_14=[$0], inv_item_sk_node_14=[$1], inv_warehouse_sk_node_14=[$2], inv_quantity_on_hand_node_14=[$3])
         :              :  +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])
         :              +- LogicalProject(ss_sold_date_sk_node_15=[$0], ss_sold_time_sk_node_15=[$1], ss_item_sk_node_15=[$2], ss_customer_sk_node_15=[$3], ss_cdemo_sk_node_15=[$4], ss_hdemo_sk_node_15=[$5], ss_addr_sk_node_15=[$6], ss_store_sk_node_15=[$7], ss_promo_sk_node_15=[$8], ss_ticket_number_node_15=[$9], ss_quantity_node_15=[$10], ss_wholesale_cost_node_15=[$11], ss_list_price_node_15=[$12], ss_sales_price_node_15=[$13], ss_ext_discount_amt_node_15=[$14], ss_ext_sales_price_node_15=[$15], ss_ext_wholesale_cost_node_15=[$16], ss_ext_list_price_node_15=[$17], ss_ext_tax_node_15=[$18], ss_coupon_amt_node_15=[$19], ss_net_paid_node_15=[$20], ss_net_paid_inc_tax_node_15=[$21], ss_net_profit_node_15=[$22])
         :                 +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
         +- LogicalProject(ss_quantity_node_16=[$1], _c1=[_UTF-16LE'hello'])
            +- LogicalAggregate(group=[{0}], EXPR$0=[SUM($0)])
               +- LogicalProject(ss_quantity_node_16=[$10])
                  +- LogicalFilter(condition=[>=($10, -45)])
                     +- LogicalProject(ss_sold_date_sk_node_16=[$0], ss_sold_time_sk_node_16=[$1], ss_item_sk_node_16=[$2], ss_customer_sk_node_16=[$3], ss_cdemo_sk_node_16=[$4], ss_hdemo_sk_node_16=[$5], ss_addr_sk_node_16=[$6], ss_store_sk_node_16=[$7], ss_promo_sk_node_16=[$8], ss_ticket_number_node_16=[$9], ss_quantity_node_16=[$10], ss_wholesale_cost_node_16=[$11], ss_list_price_node_16=[$12], ss_sales_price_node_16=[$13], ss_ext_discount_amt_node_16=[$14], ss_ext_sales_price_node_16=[$15], ss_ext_wholesale_cost_node_16=[$16], ss_ext_list_price_node_16=[$17], ss_ext_tax_node_16=[$18], ss_coupon_amt_node_16=[$19], ss_net_paid_node_16=[$20], ss_net_paid_inc_tax_node_16=[$21], ss_net_profit_node_16=[$22])
                        +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0], where=[>(EXPR$0, -21)])
+- HashAggregate(isMerge=[true], groupBy=[cs_catalog_page_sk_node_13], select=[cs_catalog_page_sk_node_13, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cs_catalog_page_sk_node_13]])
      +- LocalHashAggregate(groupBy=[cs_catalog_page_sk_node_13], select=[cs_catalog_page_sk_node_13, Partial_MIN(cs_item_sk_node_13) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_quantity_node_16, cs_call_center_sk_node_13)], select=[jygr5, cs_sold_time_sk_node_13, cs_ship_date_sk_node_13, cs_bill_customer_sk_node_13, cs_bill_cdemo_sk_node_13, cs_bill_hdemo_sk_node_13, cs_bill_addr_sk_node_13, cs_ship_customer_sk_node_13, cs_ship_cdemo_sk_node_13, cs_ship_hdemo_sk_node_13, cs_ship_addr_sk_node_13, cs_call_center_sk_node_13, cs_catalog_page_sk_node_13, cs_ship_mode_sk_node_13, cs_warehouse_sk_node_13, cs_item_sk_node_13, cs_promo_sk_node_13, cs_order_number_node_13, cs_quantity_node_13, cs_wholesale_cost_node_13, cs_list_price_node_13, cs_sales_price_node_13, cs_ext_discount_amt_node_13, cs_ext_sales_price_node_13, cs_ext_wholesale_cost_node_13, cs_ext_list_price_node_13, cs_ext_tax_node_13, cs_coupon_amt_node_13, cs_ext_ship_cost_node_13, cs_net_paid_node_13, cs_net_paid_inc_tax_node_13, cs_net_paid_inc_ship_node_13, cs_net_paid_inc_ship_tax_node_13, cs_net_profit_node_13, ss_customer_sk_node_15, ss_quantity_node_16, _c1], build=[right])
            :- Sort(orderBy=[cs_ext_discount_amt_node_13 ASC])
            :  +- Exchange(distribution=[single])
            :     +- HashJoin(joinType=[InnerJoin], where=[=(ss_customer_sk_node_15, cs_quantity_node_13)], select=[jygr5, cs_sold_time_sk_node_13, cs_ship_date_sk_node_13, cs_bill_customer_sk_node_13, cs_bill_cdemo_sk_node_13, cs_bill_hdemo_sk_node_13, cs_bill_addr_sk_node_13, cs_ship_customer_sk_node_13, cs_ship_cdemo_sk_node_13, cs_ship_hdemo_sk_node_13, cs_ship_addr_sk_node_13, cs_call_center_sk_node_13, cs_catalog_page_sk_node_13, cs_ship_mode_sk_node_13, cs_warehouse_sk_node_13, cs_item_sk_node_13, cs_promo_sk_node_13, cs_order_number_node_13, cs_quantity_node_13, cs_wholesale_cost_node_13, cs_list_price_node_13, cs_sales_price_node_13, cs_ext_discount_amt_node_13, cs_ext_sales_price_node_13, cs_ext_wholesale_cost_node_13, cs_ext_list_price_node_13, cs_ext_tax_node_13, cs_coupon_amt_node_13, cs_ext_ship_cost_node_13, cs_net_paid_node_13, cs_net_paid_inc_tax_node_13, cs_net_paid_inc_ship_node_13, cs_net_paid_inc_ship_tax_node_13, cs_net_profit_node_13, ss_customer_sk_node_15], build=[right])
            :        :- Exchange(distribution=[hash[cs_quantity_node_13]])
            :        :  +- Calc(select=[cs_sold_date_sk AS jygr5, cs_sold_time_sk AS cs_sold_time_sk_node_13, cs_ship_date_sk AS cs_ship_date_sk_node_13, cs_bill_customer_sk AS cs_bill_customer_sk_node_13, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_13, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_13, cs_bill_addr_sk AS cs_bill_addr_sk_node_13, cs_ship_customer_sk AS cs_ship_customer_sk_node_13, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_13, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_13, cs_ship_addr_sk AS cs_ship_addr_sk_node_13, cs_call_center_sk AS cs_call_center_sk_node_13, cs_catalog_page_sk AS cs_catalog_page_sk_node_13, cs_ship_mode_sk AS cs_ship_mode_sk_node_13, cs_warehouse_sk AS cs_warehouse_sk_node_13, cs_item_sk AS cs_item_sk_node_13, cs_promo_sk AS cs_promo_sk_node_13, cs_order_number AS cs_order_number_node_13, cs_quantity AS cs_quantity_node_13, cs_wholesale_cost AS cs_wholesale_cost_node_13, cs_list_price AS cs_list_price_node_13, cs_sales_price AS cs_sales_price_node_13, cs_ext_discount_amt AS cs_ext_discount_amt_node_13, cs_ext_sales_price AS cs_ext_sales_price_node_13, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_13, cs_ext_list_price AS cs_ext_list_price_node_13, cs_ext_tax AS cs_ext_tax_node_13, cs_coupon_amt AS cs_coupon_amt_node_13, cs_ext_ship_cost AS cs_ext_ship_cost_node_13, cs_net_paid AS cs_net_paid_node_13, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_13, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_13, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_13, cs_net_profit AS cs_net_profit_node_13])
            :        :     +- HashAggregate(isMerge=[false], groupBy=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
            :        :        +- Exchange(distribution=[hash[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit]])
            :        :           +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
            :        +- Exchange(distribution=[hash[ss_customer_sk_node_15]])
            :           +- Calc(select=[EXPR$0 AS ss_customer_sk_node_15])
            :              +- HashAggregate(isMerge=[true], groupBy=[ss_sold_date_sk], select=[ss_sold_date_sk, Final_MIN(min$0) AS EXPR$0])
            :                 +- Exchange(distribution=[hash[ss_sold_date_sk]])
            :                    +- LocalHashAggregate(groupBy=[ss_sold_date_sk], select=[ss_sold_date_sk, Partial_MIN(ss_customer_sk) AS min$0])
            :                       +- HashJoin(joinType=[InnerJoin], where=[=(inv_warehouse_sk, ss_hdemo_sk)], select=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
            :                          :- Exchange(distribution=[hash[inv_warehouse_sk]])
            :                          :  +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
            :                          +- Exchange(distribution=[hash[ss_hdemo_sk]])
            :                             +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[EXPR$0 AS ss_quantity_node_16, 'hello' AS _c1])
                  +- HashAggregate(isMerge=[true], groupBy=[ss_quantity], select=[ss_quantity, Final_SUM(sum$0) AS EXPR$0])
                     +- Exchange(distribution=[hash[ss_quantity]])
                        +- LocalHashAggregate(groupBy=[ss_quantity], select=[ss_quantity, Partial_SUM(ss_quantity) AS sum$0])
                           +- Calc(select=[ss_quantity], where=[>=(ss_quantity, -45)])
                              +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[>=(ss_quantity, -45)], project=[ss_quantity], metadata=[]]], fields=[ss_quantity])

== Optimized Execution Plan ==
Calc(select=[EXPR$0], where=[(EXPR$0 > -21)])
+- HashAggregate(isMerge=[true], groupBy=[cs_catalog_page_sk_node_13], select=[cs_catalog_page_sk_node_13, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cs_catalog_page_sk_node_13]])
      +- LocalHashAggregate(groupBy=[cs_catalog_page_sk_node_13], select=[cs_catalog_page_sk_node_13, Partial_MIN(cs_item_sk_node_13) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_quantity_node_16 = cs_call_center_sk_node_13)], select=[jygr5, cs_sold_time_sk_node_13, cs_ship_date_sk_node_13, cs_bill_customer_sk_node_13, cs_bill_cdemo_sk_node_13, cs_bill_hdemo_sk_node_13, cs_bill_addr_sk_node_13, cs_ship_customer_sk_node_13, cs_ship_cdemo_sk_node_13, cs_ship_hdemo_sk_node_13, cs_ship_addr_sk_node_13, cs_call_center_sk_node_13, cs_catalog_page_sk_node_13, cs_ship_mode_sk_node_13, cs_warehouse_sk_node_13, cs_item_sk_node_13, cs_promo_sk_node_13, cs_order_number_node_13, cs_quantity_node_13, cs_wholesale_cost_node_13, cs_list_price_node_13, cs_sales_price_node_13, cs_ext_discount_amt_node_13, cs_ext_sales_price_node_13, cs_ext_wholesale_cost_node_13, cs_ext_list_price_node_13, cs_ext_tax_node_13, cs_coupon_amt_node_13, cs_ext_ship_cost_node_13, cs_net_paid_node_13, cs_net_paid_inc_tax_node_13, cs_net_paid_inc_ship_node_13, cs_net_paid_inc_ship_tax_node_13, cs_net_profit_node_13, ss_customer_sk_node_15, ss_quantity_node_16, _c1], build=[right])
            :- Sort(orderBy=[cs_ext_discount_amt_node_13 ASC])
            :  +- Exchange(distribution=[single])
            :     +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ss_customer_sk_node_15 = cs_quantity_node_13)], select=[jygr5, cs_sold_time_sk_node_13, cs_ship_date_sk_node_13, cs_bill_customer_sk_node_13, cs_bill_cdemo_sk_node_13, cs_bill_hdemo_sk_node_13, cs_bill_addr_sk_node_13, cs_ship_customer_sk_node_13, cs_ship_cdemo_sk_node_13, cs_ship_hdemo_sk_node_13, cs_ship_addr_sk_node_13, cs_call_center_sk_node_13, cs_catalog_page_sk_node_13, cs_ship_mode_sk_node_13, cs_warehouse_sk_node_13, cs_item_sk_node_13, cs_promo_sk_node_13, cs_order_number_node_13, cs_quantity_node_13, cs_wholesale_cost_node_13, cs_list_price_node_13, cs_sales_price_node_13, cs_ext_discount_amt_node_13, cs_ext_sales_price_node_13, cs_ext_wholesale_cost_node_13, cs_ext_list_price_node_13, cs_ext_tax_node_13, cs_coupon_amt_node_13, cs_ext_ship_cost_node_13, cs_net_paid_node_13, cs_net_paid_inc_tax_node_13, cs_net_paid_inc_ship_node_13, cs_net_paid_inc_ship_tax_node_13, cs_net_profit_node_13, ss_customer_sk_node_15], build=[right])
            :        :- Exchange(distribution=[hash[cs_quantity_node_13]])
            :        :  +- Calc(select=[cs_sold_date_sk AS jygr5, cs_sold_time_sk AS cs_sold_time_sk_node_13, cs_ship_date_sk AS cs_ship_date_sk_node_13, cs_bill_customer_sk AS cs_bill_customer_sk_node_13, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_13, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_13, cs_bill_addr_sk AS cs_bill_addr_sk_node_13, cs_ship_customer_sk AS cs_ship_customer_sk_node_13, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_13, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_13, cs_ship_addr_sk AS cs_ship_addr_sk_node_13, cs_call_center_sk AS cs_call_center_sk_node_13, cs_catalog_page_sk AS cs_catalog_page_sk_node_13, cs_ship_mode_sk AS cs_ship_mode_sk_node_13, cs_warehouse_sk AS cs_warehouse_sk_node_13, cs_item_sk AS cs_item_sk_node_13, cs_promo_sk AS cs_promo_sk_node_13, cs_order_number AS cs_order_number_node_13, cs_quantity AS cs_quantity_node_13, cs_wholesale_cost AS cs_wholesale_cost_node_13, cs_list_price AS cs_list_price_node_13, cs_sales_price AS cs_sales_price_node_13, cs_ext_discount_amt AS cs_ext_discount_amt_node_13, cs_ext_sales_price AS cs_ext_sales_price_node_13, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_13, cs_ext_list_price AS cs_ext_list_price_node_13, cs_ext_tax AS cs_ext_tax_node_13, cs_coupon_amt AS cs_coupon_amt_node_13, cs_ext_ship_cost AS cs_ext_ship_cost_node_13, cs_net_paid AS cs_net_paid_node_13, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_13, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_13, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_13, cs_net_profit AS cs_net_profit_node_13])
            :        :     +- HashAggregate(isMerge=[false], groupBy=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
            :        :        +- Exchange(distribution=[hash[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit]])
            :        :           +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
            :        +- Exchange(distribution=[hash[ss_customer_sk_node_15]])
            :           +- Calc(select=[EXPR$0 AS ss_customer_sk_node_15])
            :              +- HashAggregate(isMerge=[true], groupBy=[ss_sold_date_sk], select=[ss_sold_date_sk, Final_MIN(min$0) AS EXPR$0])
            :                 +- Exchange(distribution=[hash[ss_sold_date_sk]])
            :                    +- LocalHashAggregate(groupBy=[ss_sold_date_sk], select=[ss_sold_date_sk, Partial_MIN(ss_customer_sk) AS min$0])
            :                       +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(inv_warehouse_sk = ss_hdemo_sk)], select=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
            :                          :- Exchange(distribution=[hash[inv_warehouse_sk]])
            :                          :  +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
            :                          +- Exchange(distribution=[hash[ss_hdemo_sk]])
            :                             +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[EXPR$0 AS ss_quantity_node_16, 'hello' AS _c1])
                  +- HashAggregate(isMerge=[true], groupBy=[ss_quantity], select=[ss_quantity, Final_SUM(sum$0) AS EXPR$0])
                     +- Exchange(distribution=[hash[ss_quantity]])
                        +- LocalHashAggregate(groupBy=[ss_quantity], select=[ss_quantity, Partial_SUM(ss_quantity) AS sum$0])
                           +- Calc(select=[ss_quantity], where=[(ss_quantity >= -45)])
                              +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[>=(ss_quantity, -45)], project=[ss_quantity], metadata=[]]], fields=[ss_quantity])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o219946193.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#445162652:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[22](input=RelSubset#445162650,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[22]), rel#445162649:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[22](input=RelSubset#445162648,groupBy=cs_call_center_sk_node_13, cs_catalog_page_sk_node_13,select=cs_call_center_sk_node_13, cs_catalog_page_sk_node_13, Partial_MIN(cs_item_sk_node_13) AS min$0)]
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