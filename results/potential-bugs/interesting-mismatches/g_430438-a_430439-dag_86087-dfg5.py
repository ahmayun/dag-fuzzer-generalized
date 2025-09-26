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

autonode_10 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_13 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_12 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_11 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_7 = autonode_10.distinct()
autonode_9 = autonode_13.select(col('cc_call_center_id_node_13'))
autonode_8 = autonode_11.join(autonode_12, col('i_manager_id_node_12') == col('ws_promo_sk_node_11'))
autonode_5 = autonode_7.filter(col('ws_promo_sk_node_10') >= -49)
autonode_6 = autonode_8.join(autonode_9, col('cc_call_center_id_node_13') == col('i_brand_node_12'))
autonode_3 = autonode_5.order_by(col('ws_promo_sk_node_10'))
autonode_4 = autonode_6.select(col('ws_quantity_node_11'))
autonode_2 = autonode_3.join(autonode_4, col('ws_quantity_node_11') == col('ws_bill_hdemo_sk_node_10'))
autonode_1 = autonode_2.group_by(col('ws_bill_hdemo_sk_node_10')).select(col('ws_ship_date_sk_node_10').max.alias('ws_ship_date_sk_node_10'))
sink = autonode_1.select(col('ws_ship_date_sk_node_10'))
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
LogicalProject(ws_ship_date_sk_node_10=[$1])
+- LogicalAggregate(group=[{6}], EXPR$0=[MAX($2)])
   +- LogicalJoin(condition=[=($34, $6)], joinType=[inner])
      :- LogicalSort(sort0=[$16], dir0=[ASC])
      :  +- LogicalFilter(condition=[>=($16, -49)])
      :     +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33}])
      :        +- LogicalProject(ws_sold_date_sk_node_10=[$0], ws_sold_time_sk_node_10=[$1], ws_ship_date_sk_node_10=[$2], ws_item_sk_node_10=[$3], ws_bill_customer_sk_node_10=[$4], ws_bill_cdemo_sk_node_10=[$5], ws_bill_hdemo_sk_node_10=[$6], ws_bill_addr_sk_node_10=[$7], ws_ship_customer_sk_node_10=[$8], ws_ship_cdemo_sk_node_10=[$9], ws_ship_hdemo_sk_node_10=[$10], ws_ship_addr_sk_node_10=[$11], ws_web_page_sk_node_10=[$12], ws_web_site_sk_node_10=[$13], ws_ship_mode_sk_node_10=[$14], ws_warehouse_sk_node_10=[$15], ws_promo_sk_node_10=[$16], ws_order_number_node_10=[$17], ws_quantity_node_10=[$18], ws_wholesale_cost_node_10=[$19], ws_list_price_node_10=[$20], ws_sales_price_node_10=[$21], ws_ext_discount_amt_node_10=[$22], ws_ext_sales_price_node_10=[$23], ws_ext_wholesale_cost_node_10=[$24], ws_ext_list_price_node_10=[$25], ws_ext_tax_node_10=[$26], ws_coupon_amt_node_10=[$27], ws_ext_ship_cost_node_10=[$28], ws_net_paid_node_10=[$29], ws_net_paid_inc_tax_node_10=[$30], ws_net_paid_inc_ship_node_10=[$31], ws_net_paid_inc_ship_tax_node_10=[$32], ws_net_profit_node_10=[$33])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
      +- LogicalProject(ws_quantity_node_11=[$18])
         +- LogicalJoin(condition=[=($56, $42)], joinType=[inner])
            :- LogicalJoin(condition=[=($54, $16)], joinType=[inner])
            :  :- LogicalProject(ws_sold_date_sk_node_11=[$0], ws_sold_time_sk_node_11=[$1], ws_ship_date_sk_node_11=[$2], ws_item_sk_node_11=[$3], ws_bill_customer_sk_node_11=[$4], ws_bill_cdemo_sk_node_11=[$5], ws_bill_hdemo_sk_node_11=[$6], ws_bill_addr_sk_node_11=[$7], ws_ship_customer_sk_node_11=[$8], ws_ship_cdemo_sk_node_11=[$9], ws_ship_hdemo_sk_node_11=[$10], ws_ship_addr_sk_node_11=[$11], ws_web_page_sk_node_11=[$12], ws_web_site_sk_node_11=[$13], ws_ship_mode_sk_node_11=[$14], ws_warehouse_sk_node_11=[$15], ws_promo_sk_node_11=[$16], ws_order_number_node_11=[$17], ws_quantity_node_11=[$18], ws_wholesale_cost_node_11=[$19], ws_list_price_node_11=[$20], ws_sales_price_node_11=[$21], ws_ext_discount_amt_node_11=[$22], ws_ext_sales_price_node_11=[$23], ws_ext_wholesale_cost_node_11=[$24], ws_ext_list_price_node_11=[$25], ws_ext_tax_node_11=[$26], ws_coupon_amt_node_11=[$27], ws_ext_ship_cost_node_11=[$28], ws_net_paid_node_11=[$29], ws_net_paid_inc_tax_node_11=[$30], ws_net_paid_inc_ship_node_11=[$31], ws_net_paid_inc_ship_tax_node_11=[$32], ws_net_profit_node_11=[$33])
            :  :  +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
            :  +- LogicalProject(i_item_sk_node_12=[$0], i_item_id_node_12=[$1], i_rec_start_date_node_12=[$2], i_rec_end_date_node_12=[$3], i_item_desc_node_12=[$4], i_current_price_node_12=[$5], i_wholesale_cost_node_12=[$6], i_brand_id_node_12=[$7], i_brand_node_12=[$8], i_class_id_node_12=[$9], i_class_node_12=[$10], i_category_id_node_12=[$11], i_category_node_12=[$12], i_manufact_id_node_12=[$13], i_manufact_node_12=[$14], i_size_node_12=[$15], i_formulation_node_12=[$16], i_color_node_12=[$17], i_units_node_12=[$18], i_container_node_12=[$19], i_manager_id_node_12=[$20], i_product_name_node_12=[$21])
            :     +- LogicalTableScan(table=[[default_catalog, default_database, item]])
            +- LogicalProject(cc_call_center_id_node_13=[$1])
               +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ws_ship_date_sk_node_10])
+- HashAggregate(isMerge=[true], groupBy=[ws_bill_hdemo_sk], select=[ws_bill_hdemo_sk, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ws_bill_hdemo_sk]])
      +- LocalHashAggregate(groupBy=[ws_bill_hdemo_sk], select=[ws_bill_hdemo_sk, Partial_MAX(ws_ship_date_sk) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(ws_quantity_node_11, ws_bill_hdemo_sk)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, ws_quantity_node_11], isBroadcast=[true], build=[right])
            :- Sort(orderBy=[ws_promo_sk ASC])
            :  +- Exchange(distribution=[single])
            :     +- HashAggregate(isMerge=[false], groupBy=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
            :        +- Exchange(distribution=[hash[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit]])
            :           +- Calc(select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], where=[>=(ws_promo_sk, -49)])
            :              +- TableSourceScan(table=[[default_catalog, default_database, web_sales, filter=[>=(ws_promo_sk, -49)]]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[ws_quantity_node_10 AS ws_quantity_node_11])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cc_call_center_id, i_brand_node_12)], select=[ws_quantity_node_10, i_brand_node_12, cc_call_center_id], build=[right])
                     :- Calc(select=[ws_quantity AS ws_quantity_node_10, i_brand AS i_brand_node_12])
                     :  +- HashJoin(joinType=[InnerJoin], where=[=(i_manager_id, ws_promo_sk)], select=[ws_promo_sk, ws_quantity, i_brand, i_manager_id], isBroadcast=[true], build=[right])
                     :     :- TableSourceScan(table=[[default_catalog, default_database, web_sales, project=[ws_promo_sk, ws_quantity], metadata=[]]], fields=[ws_promo_sk, ws_quantity])
                     :     +- Exchange(distribution=[broadcast])
                     :        +- TableSourceScan(table=[[default_catalog, default_database, item, project=[i_brand, i_manager_id], metadata=[]]], fields=[i_brand, i_manager_id])
                     +- Exchange(distribution=[broadcast])
                        +- TableSourceScan(table=[[default_catalog, default_database, call_center, project=[cc_call_center_id], metadata=[]]], fields=[cc_call_center_id])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ws_ship_date_sk_node_10])
+- HashAggregate(isMerge=[true], groupBy=[ws_bill_hdemo_sk], select=[ws_bill_hdemo_sk, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ws_bill_hdemo_sk]])
      +- LocalHashAggregate(groupBy=[ws_bill_hdemo_sk], select=[ws_bill_hdemo_sk, Partial_MAX(ws_ship_date_sk) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[(ws_quantity_node_11 = ws_bill_hdemo_sk)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, ws_quantity_node_11], isBroadcast=[true], build=[right])
            :- Sort(orderBy=[ws_promo_sk ASC])
            :  +- Exchange(distribution=[single])
            :     +- HashAggregate(isMerge=[false], groupBy=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
            :        +- Exchange(distribution=[hash[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit]])
            :           +- Calc(select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], where=[(ws_promo_sk >= -49)])
            :              +- TableSourceScan(table=[[default_catalog, default_database, web_sales, filter=[>=(ws_promo_sk, -49)]]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[ws_quantity_node_10 AS ws_quantity_node_11])
                  +- MultipleInput(readOrder=[0,1,0], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(cc_call_center_id = i_brand_node_12)], select=[ws_quantity_node_10, i_brand_node_12, cc_call_center_id], build=[right])\
:- Calc(select=[ws_quantity AS ws_quantity_node_10, i_brand AS i_brand_node_12])\
:  +- HashJoin(joinType=[InnerJoin], where=[(i_manager_id = ws_promo_sk)], select=[ws_promo_sk, ws_quantity, i_brand, i_manager_id], isBroadcast=[true], build=[right])\
:     :- [#2] TableSourceScan(table=[[default_catalog, default_database, web_sales, project=[ws_promo_sk, ws_quantity], metadata=[]]], fields=[ws_promo_sk, ws_quantity])\
:     +- [#3] Exchange(distribution=[broadcast])\
+- [#1] Exchange(distribution=[broadcast])\
])
                     :- Exchange(distribution=[broadcast])
                     :  +- TableSourceScan(table=[[default_catalog, default_database, call_center, project=[cc_call_center_id], metadata=[]]], fields=[cc_call_center_id])
                     :- TableSourceScan(table=[[default_catalog, default_database, web_sales, project=[ws_promo_sk, ws_quantity], metadata=[]]], fields=[ws_promo_sk, ws_quantity])
                     +- Exchange(distribution=[broadcast])
                        +- TableSourceScan(table=[[default_catalog, default_database, item, project=[i_brand, i_manager_id], metadata=[]]], fields=[i_brand, i_manager_id])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o234384961.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#474058280:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[16](input=RelSubset#474058278,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[16]), rel#474058277:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[16](input=RelSubset#474058276,groupBy=ws_bill_hdemo_sk,select=ws_bill_hdemo_sk, Partial_MAX(ws_ship_date_sk) AS max$0)]
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