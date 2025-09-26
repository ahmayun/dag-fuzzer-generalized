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
    return values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_9 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_8 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_7 = autonode_10.order_by(col('cr_return_quantity_node_10'))
autonode_6 = autonode_9.group_by(col('t_sub_shift_node_9')).select(col('t_time_node_9').sum.alias('t_time_node_9'))
autonode_5 = autonode_8.distinct()
autonode_4 = autonode_6.join(autonode_7, col('t_time_node_9') == col('cr_refunded_hdemo_sk_node_10'))
autonode_3 = autonode_5.add_columns(lit("hello"))
autonode_2 = autonode_4.group_by(col('cr_reason_sk_node_10')).select(col('cr_refunded_cdemo_sk_node_10').min.alias('cr_refunded_cdemo_sk_node_10'))
autonode_1 = autonode_3.group_by(col('i_current_price_node_8')).select(col('i_class_id_node_8').sum.alias('i_class_id_node_8'))
sink = autonode_1.join(autonode_2, col('i_class_id_node_8') == col('cr_refunded_cdemo_sk_node_10'))
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
LogicalJoin(condition=[=($0, $1)], joinType=[inner])
:- LogicalProject(i_class_id_node_8=[$1])
:  +- LogicalAggregate(group=[{0}], EXPR$0=[SUM($1)])
:     +- LogicalProject(i_current_price_node_8=[$5], i_class_id_node_8=[$9])
:        +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}])
:           +- LogicalProject(i_item_sk_node_8=[$0], i_item_id_node_8=[$1], i_rec_start_date_node_8=[$2], i_rec_end_date_node_8=[$3], i_item_desc_node_8=[$4], i_current_price_node_8=[$5], i_wholesale_cost_node_8=[$6], i_brand_id_node_8=[$7], i_brand_node_8=[$8], i_class_id_node_8=[$9], i_class_node_8=[$10], i_category_id_node_8=[$11], i_category_node_8=[$12], i_manufact_id_node_8=[$13], i_manufact_node_8=[$14], i_size_node_8=[$15], i_formulation_node_8=[$16], i_color_node_8=[$17], i_units_node_8=[$18], i_container_node_8=[$19], i_manager_id_node_8=[$20], i_product_name_node_8=[$21])
:              +- LogicalTableScan(table=[[default_catalog, default_database, item]])
+- LogicalProject(cr_refunded_cdemo_sk_node_10=[$1])
   +- LogicalAggregate(group=[{16}], EXPR$0=[MIN($5)])
      +- LogicalJoin(condition=[=($0, $6)], joinType=[inner])
         :- LogicalProject(t_time_node_9=[$1])
         :  +- LogicalAggregate(group=[{1}], EXPR$0=[SUM($0)])
         :     +- LogicalProject(t_time_node_9=[$2], t_sub_shift_node_9=[$8])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
         +- LogicalSort(sort0=[$17], dir0=[ASC])
            +- LogicalProject(cr_returned_date_sk_node_10=[$0], cr_returned_time_sk_node_10=[$1], cr_item_sk_node_10=[$2], cr_refunded_customer_sk_node_10=[$3], cr_refunded_cdemo_sk_node_10=[$4], cr_refunded_hdemo_sk_node_10=[$5], cr_refunded_addr_sk_node_10=[$6], cr_returning_customer_sk_node_10=[$7], cr_returning_cdemo_sk_node_10=[$8], cr_returning_hdemo_sk_node_10=[$9], cr_returning_addr_sk_node_10=[$10], cr_call_center_sk_node_10=[$11], cr_catalog_page_sk_node_10=[$12], cr_ship_mode_sk_node_10=[$13], cr_warehouse_sk_node_10=[$14], cr_reason_sk_node_10=[$15], cr_order_number_node_10=[$16], cr_return_quantity_node_10=[$17], cr_return_amount_node_10=[$18], cr_return_tax_node_10=[$19], cr_return_amt_inc_tax_node_10=[$20], cr_fee_node_10=[$21], cr_return_ship_cost_node_10=[$22], cr_refunded_cash_node_10=[$23], cr_reversed_charge_node_10=[$24], cr_store_credit_node_10=[$25], cr_net_loss_node_10=[$26])
               +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])

== Optimized Physical Plan ==
HashJoin(joinType=[InnerJoin], where=[=(i_class_id_node_8, cr_refunded_cdemo_sk_node_10)], select=[i_class_id_node_8, cr_refunded_cdemo_sk_node_10], isBroadcast=[true], build=[left])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[EXPR$0 AS i_class_id_node_8])
:     +- HashAggregate(isMerge=[true], groupBy=[i_current_price_node_8], select=[i_current_price_node_8, Final_SUM(sum$0) AS EXPR$0])
:        +- Exchange(distribution=[hash[i_current_price_node_8]])
:           +- LocalHashAggregate(groupBy=[i_current_price_node_8], select=[i_current_price_node_8, Partial_SUM(i_class_id_node_8) AS sum$0])
:              +- Calc(select=[i_current_price AS i_current_price_node_8, i_class_id AS i_class_id_node_8])
:                 +- HashAggregate(isMerge=[false], groupBy=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
:                    +- Exchange(distribution=[hash[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name]])
:                       +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
+- Calc(select=[EXPR$0 AS cr_refunded_cdemo_sk_node_10])
   +- HashAggregate(isMerge=[true], groupBy=[cr_reason_sk], select=[cr_reason_sk, Final_MIN(min$0) AS EXPR$0])
      +- Exchange(distribution=[hash[cr_reason_sk]])
         +- LocalHashAggregate(groupBy=[cr_reason_sk], select=[cr_reason_sk, Partial_MIN(cr_refunded_cdemo_sk) AS min$0])
            +- HashJoin(joinType=[InnerJoin], where=[=(t_time_node_9, cr_refunded_hdemo_sk)], select=[t_time_node_9, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], isBroadcast=[true], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[EXPR$0 AS t_time_node_9])
               :     +- HashAggregate(isMerge=[true], groupBy=[t_sub_shift], select=[t_sub_shift, Final_SUM(sum$0) AS EXPR$0])
               :        +- Exchange(distribution=[hash[t_sub_shift]])
               :           +- LocalHashAggregate(groupBy=[t_sub_shift], select=[t_sub_shift, Partial_SUM(t_time) AS sum$0])
               :              +- TableSourceScan(table=[[default_catalog, default_database, time_dim, project=[t_time, t_sub_shift], metadata=[]]], fields=[t_time, t_sub_shift])
               +- Sort(orderBy=[cr_return_quantity ASC])
                  +- Exchange(distribution=[single])
                     +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

== Optimized Execution Plan ==
MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(i_class_id_node_8 = cr_refunded_cdemo_sk_node_10)], select=[i_class_id_node_8, cr_refunded_cdemo_sk_node_10], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- Calc(select=[EXPR$0 AS cr_refunded_cdemo_sk_node_10])\
   +- HashAggregate(isMerge=[true], groupBy=[cr_reason_sk], select=[cr_reason_sk, Final_MIN(min$0) AS EXPR$0])\
      +- [#2] Exchange(distribution=[hash[cr_reason_sk]])\
])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[EXPR$0 AS i_class_id_node_8])
:     +- HashAggregate(isMerge=[true], groupBy=[i_current_price_node_8], select=[i_current_price_node_8, Final_SUM(sum$0) AS EXPR$0])
:        +- Exchange(distribution=[hash[i_current_price_node_8]])
:           +- LocalHashAggregate(groupBy=[i_current_price_node_8], select=[i_current_price_node_8, Partial_SUM(i_class_id_node_8) AS sum$0])
:              +- Calc(select=[i_current_price AS i_current_price_node_8, i_class_id AS i_class_id_node_8])
:                 +- HashAggregate(isMerge=[false], groupBy=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
:                    +- Exchange(distribution=[hash[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name]])
:                       +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
+- Exchange(distribution=[hash[cr_reason_sk]])
   +- LocalHashAggregate(groupBy=[cr_reason_sk], select=[cr_reason_sk, Partial_MIN(cr_refunded_cdemo_sk) AS min$0])
      +- HashJoin(joinType=[InnerJoin], where=[(t_time_node_9 = cr_refunded_hdemo_sk)], select=[t_time_node_9, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], isBroadcast=[true], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[EXPR$0 AS t_time_node_9])
         :     +- HashAggregate(isMerge=[true], groupBy=[t_sub_shift], select=[t_sub_shift, Final_SUM(sum$0) AS EXPR$0])
         :        +- Exchange(distribution=[hash[t_sub_shift]])
         :           +- LocalHashAggregate(groupBy=[t_sub_shift], select=[t_sub_shift, Partial_SUM(t_time) AS sum$0])
         :              +- TableSourceScan(table=[[default_catalog, default_database, time_dim, project=[t_time, t_sub_shift], metadata=[]]], fields=[t_time, t_sub_shift])
         +- Sort(orderBy=[cr_return_quantity ASC])
            +- Exchange(distribution=[single])
               +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o532766.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#1207029:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[17](input=RelSubset#1207027,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[17]), rel#1207026:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[17](input=RelSubset#1207025,groupBy=cr_refunded_hdemo_sk, cr_reason_sk,select=cr_refunded_hdemo_sk, cr_reason_sk, Partial_MIN(cr_refunded_cdemo_sk) AS min$0)]
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