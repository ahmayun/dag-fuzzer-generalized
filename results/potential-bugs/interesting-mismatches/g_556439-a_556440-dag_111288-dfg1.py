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
autonode_9 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_11 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_7 = autonode_10.order_by(col('cs_ship_cdemo_sk_node_10'))
autonode_6 = autonode_9.alias('3ACW3')
autonode_8 = autonode_11.alias('wcJOa')
autonode_4 = autonode_6.join(autonode_7, col('i_current_price_node_9') == col('cs_ext_wholesale_cost_node_10'))
autonode_5 = autonode_8.limit(59)
autonode_2 = autonode_4.group_by(col('cs_quantity_node_10')).select(col('i_item_desc_node_9').min.alias('i_item_desc_node_9'))
autonode_3 = autonode_5.order_by(col('sm_ship_mode_id_node_11'))
autonode_1 = autonode_2.join(autonode_3, col('sm_code_node_11') == col('i_item_desc_node_9'))
sink = autonode_1.filter(col('sm_carrier_node_11').char_length >= 5)
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
LogicalFilter(condition=[>=(CHAR_LENGTH($5), 5)])
+- LogicalJoin(condition=[=($4, $0)], joinType=[inner])
   :- LogicalProject(i_item_desc_node_9=[$1])
   :  +- LogicalAggregate(group=[{40}], EXPR$0=[MIN($4)])
   :     +- LogicalJoin(condition=[=($5, $46)], joinType=[inner])
   :        :- LogicalProject(3ACW3=[AS($0, _UTF-16LE'3ACW3')], i_item_id_node_9=[$1], i_rec_start_date_node_9=[$2], i_rec_end_date_node_9=[$3], i_item_desc_node_9=[$4], i_current_price_node_9=[$5], i_wholesale_cost_node_9=[$6], i_brand_id_node_9=[$7], i_brand_node_9=[$8], i_class_id_node_9=[$9], i_class_node_9=[$10], i_category_id_node_9=[$11], i_category_node_9=[$12], i_manufact_id_node_9=[$13], i_manufact_node_9=[$14], i_size_node_9=[$15], i_formulation_node_9=[$16], i_color_node_9=[$17], i_units_node_9=[$18], i_container_node_9=[$19], i_manager_id_node_9=[$20], i_product_name_node_9=[$21])
   :        :  +- LogicalTableScan(table=[[default_catalog, default_database, item]])
   :        +- LogicalSort(sort0=[$8], dir0=[ASC])
   :           +- LogicalProject(cs_sold_date_sk_node_10=[$0], cs_sold_time_sk_node_10=[$1], cs_ship_date_sk_node_10=[$2], cs_bill_customer_sk_node_10=[$3], cs_bill_cdemo_sk_node_10=[$4], cs_bill_hdemo_sk_node_10=[$5], cs_bill_addr_sk_node_10=[$6], cs_ship_customer_sk_node_10=[$7], cs_ship_cdemo_sk_node_10=[$8], cs_ship_hdemo_sk_node_10=[$9], cs_ship_addr_sk_node_10=[$10], cs_call_center_sk_node_10=[$11], cs_catalog_page_sk_node_10=[$12], cs_ship_mode_sk_node_10=[$13], cs_warehouse_sk_node_10=[$14], cs_item_sk_node_10=[$15], cs_promo_sk_node_10=[$16], cs_order_number_node_10=[$17], cs_quantity_node_10=[$18], cs_wholesale_cost_node_10=[$19], cs_list_price_node_10=[$20], cs_sales_price_node_10=[$21], cs_ext_discount_amt_node_10=[$22], cs_ext_sales_price_node_10=[$23], cs_ext_wholesale_cost_node_10=[$24], cs_ext_list_price_node_10=[$25], cs_ext_tax_node_10=[$26], cs_coupon_amt_node_10=[$27], cs_ext_ship_cost_node_10=[$28], cs_net_paid_node_10=[$29], cs_net_paid_inc_tax_node_10=[$30], cs_net_paid_inc_ship_node_10=[$31], cs_net_paid_inc_ship_tax_node_10=[$32], cs_net_profit_node_10=[$33])
   :              +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
   +- LogicalSort(sort0=[$1], dir0=[ASC])
      +- LogicalSort(fetch=[59])
         +- LogicalProject(wcJOa=[AS($0, _UTF-16LE'wcJOa')], sm_ship_mode_id_node_11=[$1], sm_type_node_11=[$2], sm_code_node_11=[$3], sm_carrier_node_11=[$4], sm_contract_node_11=[$5])
            +- LogicalTableScan(table=[[default_catalog, default_database, ship_mode]])

== Optimized Physical Plan ==
HashJoin(joinType=[InnerJoin], where=[=(sm_code_node_11, i_item_desc_node_9)], select=[i_item_desc_node_9, wcJOa, sm_ship_mode_id_node_11, sm_type_node_11, sm_code_node_11, sm_carrier_node_11, sm_contract_node_11], isBroadcast=[true], build=[right])
:- Calc(select=[EXPR$0 AS i_item_desc_node_9])
:  +- SortAggregate(isMerge=[false], groupBy=[cs_quantity], select=[cs_quantity, MIN(i_item_desc_node_9) AS EXPR$0])
:     +- Sort(orderBy=[cs_quantity ASC])
:        +- Exchange(distribution=[hash[cs_quantity]])
:           +- NestedLoopJoin(joinType=[InnerJoin], where=[=(i_current_price_node_9, cs_ext_wholesale_cost)], select=[3ACW3, i_item_id_node_9, i_rec_start_date_node_9, i_rec_end_date_node_9, i_item_desc_node_9, i_current_price_node_9, i_wholesale_cost_node_9, i_brand_id_node_9, i_brand_node_9, i_class_id_node_9, i_class_node_9, i_category_id_node_9, i_category_node_9, i_manufact_id_node_9, i_manufact_node_9, i_size_node_9, i_formulation_node_9, i_color_node_9, i_units_node_9, i_container_node_9, i_manager_id_node_9, i_product_name_node_9, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], build=[left])
:              :- Exchange(distribution=[broadcast])
:              :  +- Calc(select=[i_item_sk AS 3ACW3, i_item_id AS i_item_id_node_9, i_rec_start_date AS i_rec_start_date_node_9, i_rec_end_date AS i_rec_end_date_node_9, i_item_desc AS i_item_desc_node_9, i_current_price AS i_current_price_node_9, i_wholesale_cost AS i_wholesale_cost_node_9, i_brand_id AS i_brand_id_node_9, i_brand AS i_brand_node_9, i_class_id AS i_class_id_node_9, i_class AS i_class_node_9, i_category_id AS i_category_id_node_9, i_category AS i_category_node_9, i_manufact_id AS i_manufact_id_node_9, i_manufact AS i_manufact_node_9, i_size AS i_size_node_9, i_formulation AS i_formulation_node_9, i_color AS i_color_node_9, i_units AS i_units_node_9, i_container AS i_container_node_9, i_manager_id AS i_manager_id_node_9, i_product_name AS i_product_name_node_9])
:              :     +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
:              +- Sort(orderBy=[cs_ship_cdemo_sk ASC])
:                 +- Exchange(distribution=[single])
:                    +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
+- Exchange(distribution=[broadcast])
   +- Calc(select=[sm_ship_mode_sk AS wcJOa, sm_ship_mode_id AS sm_ship_mode_id_node_11, sm_type AS sm_type_node_11, sm_code AS sm_code_node_11, sm_carrier AS sm_carrier_node_11, sm_contract AS sm_contract_node_11], where=[>=(CHAR_LENGTH(sm_carrier), 5)])
      +- Sort(orderBy=[sm_ship_mode_id ASC])
         +- Limit(offset=[0], fetch=[59], global=[true])
            +- Exchange(distribution=[single])
               +- Limit(offset=[0], fetch=[59], global=[false])
                  +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])

== Optimized Execution Plan ==
MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(sm_code_node_11 = i_item_desc_node_9)], select=[i_item_desc_node_9, wcJOa, sm_ship_mode_id_node_11, sm_type_node_11, sm_code_node_11, sm_carrier_node_11, sm_contract_node_11], isBroadcast=[true], build=[right])\
:- Calc(select=[EXPR$0 AS i_item_desc_node_9])\
:  +- SortAggregate(isMerge=[false], groupBy=[cs_quantity], select=[cs_quantity, MIN(i_item_desc_node_9) AS EXPR$0])\
:     +- Sort(orderBy=[cs_quantity ASC])\
:        +- [#2] Exchange(distribution=[hash[cs_quantity]])\
+- [#1] Exchange(distribution=[broadcast])\
])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[sm_ship_mode_sk AS wcJOa, sm_ship_mode_id AS sm_ship_mode_id_node_11, sm_type AS sm_type_node_11, sm_code AS sm_code_node_11, sm_carrier AS sm_carrier_node_11, sm_contract AS sm_contract_node_11], where=[(CHAR_LENGTH(sm_carrier) >= 5)])
:     +- Sort(orderBy=[sm_ship_mode_id ASC])
:        +- Limit(offset=[0], fetch=[59], global=[true])
:           +- Exchange(distribution=[single])
:              +- Limit(offset=[0], fetch=[59], global=[false])
:                 +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
+- Exchange(distribution=[hash[cs_quantity]])
   +- NestedLoopJoin(joinType=[InnerJoin], where=[(i_current_price_node_9 = cs_ext_wholesale_cost)], select=[3ACW3, i_item_id_node_9, i_rec_start_date_node_9, i_rec_end_date_node_9, i_item_desc_node_9, i_current_price_node_9, i_wholesale_cost_node_9, i_brand_id_node_9, i_brand_node_9, i_class_id_node_9, i_class_node_9, i_category_id_node_9, i_category_node_9, i_manufact_id_node_9, i_manufact_node_9, i_size_node_9, i_formulation_node_9, i_color_node_9, i_units_node_9, i_container_node_9, i_manager_id_node_9, i_product_name_node_9, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], build=[left])
      :- Exchange(distribution=[broadcast])
      :  +- Calc(select=[i_item_sk AS 3ACW3, i_item_id AS i_item_id_node_9, i_rec_start_date AS i_rec_start_date_node_9, i_rec_end_date AS i_rec_end_date_node_9, i_item_desc AS i_item_desc_node_9, i_current_price AS i_current_price_node_9, i_wholesale_cost AS i_wholesale_cost_node_9, i_brand_id AS i_brand_id_node_9, i_brand AS i_brand_node_9, i_class_id AS i_class_id_node_9, i_class AS i_class_node_9, i_category_id AS i_category_id_node_9, i_category AS i_category_node_9, i_manufact_id AS i_manufact_id_node_9, i_manufact AS i_manufact_node_9, i_size AS i_size_node_9, i_formulation AS i_formulation_node_9, i_color AS i_color_node_9, i_units AS i_units_node_9, i_container AS i_container_node_9, i_manager_id AS i_manager_id_node_9, i_product_name AS i_product_name_node_9])
      :     +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
      +- Sort(orderBy=[cs_ship_cdemo_sk ASC])
         +- Exchange(distribution=[single])
            +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o303342246.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#612047037:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[8](input=RelSubset#612047035,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[8]), rel#612047034:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[8](input=RelSubset#612047033,groupBy=cs_quantity, cs_ext_wholesale_cost,select=cs_quantity, cs_ext_wholesale_cost)]
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