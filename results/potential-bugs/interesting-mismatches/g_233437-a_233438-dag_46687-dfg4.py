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
    return values.var()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_9 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_8 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_7 = autonode_9.order_by(col('inv_quantity_on_hand_node_9'))
autonode_6 = autonode_8.order_by(col('i_manufact_node_8'))
autonode_5 = autonode_7.alias('6Qxhc')
autonode_4 = autonode_6.limit(15)
autonode_3 = autonode_4.join(autonode_5, col('inv_item_sk_node_9') == col('i_class_id_node_8'))
autonode_2 = autonode_3.group_by(col('i_class_id_node_8')).select(col('i_item_sk_node_8').min.alias('i_item_sk_node_8'))
autonode_1 = autonode_2.add_columns(lit("hello"))
sink = autonode_1.add_columns(lit("hello"))
print(sink.explain())

# ======== Details ========
"""
{
  "is_same": false,
  "result_name": "MismatchException",
  "result_details": {
    "opt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o127181905.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#256476949:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[14](input=RelSubset#256476947,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[14]), rel#256476946:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[14](input=RelSubset#256476945,groupBy=i_class_id,select=i_class_id, Partial_MIN(i_item_sk) AS min$0)]
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
    },
    "unopt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalProject(i_item_sk_node_8=[$1], _c1=[_UTF-16LE'hello'], _c2=[_UTF-16LE'hello'])
+- LogicalAggregate(group=[{9}], EXPR$0=[MIN($0)])
   +- LogicalJoin(condition=[=($23, $9)], joinType=[inner])
      :- LogicalSort(sort0=[$14], dir0=[ASC], fetch=[15])
      :  +- LogicalProject(i_item_sk_node_8=[$0], i_item_id_node_8=[$1], i_rec_start_date_node_8=[$2], i_rec_end_date_node_8=[$3], i_item_desc_node_8=[$4], i_current_price_node_8=[$5], i_wholesale_cost_node_8=[$6], i_brand_id_node_8=[$7], i_brand_node_8=[$8], i_class_id_node_8=[$9], i_class_node_8=[$10], i_category_id_node_8=[$11], i_category_node_8=[$12], i_manufact_id_node_8=[$13], i_manufact_node_8=[$14], i_size_node_8=[$15], i_formulation_node_8=[$16], i_color_node_8=[$17], i_units_node_8=[$18], i_container_node_8=[$19], i_manager_id_node_8=[$20], i_product_name_node_8=[$21])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, item]])
      +- LogicalProject(6Qxhc=[AS($0, _UTF-16LE'6Qxhc')], inv_item_sk_node_9=[$1], inv_warehouse_sk_node_9=[$2], inv_quantity_on_hand_node_9=[$3])
         +- LogicalSort(sort0=[$3], dir0=[ASC])
            +- LogicalProject(inv_date_sk_node_9=[$0], inv_item_sk_node_9=[$1], inv_warehouse_sk_node_9=[$2], inv_quantity_on_hand_node_9=[$3])
               +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS i_item_sk_node_8, 'hello' AS _c1, 'hello' AS _c2])
+- SortAggregate(isMerge=[true], groupBy=[i_class_id], select=[i_class_id, Final_MIN(min$0) AS EXPR$0])
   +- Sort(orderBy=[i_class_id ASC])
      +- Exchange(distribution=[hash[i_class_id]])
         +- LocalSortAggregate(groupBy=[i_class_id], select=[i_class_id, Partial_MIN(i_item_sk) AS min$0])
            +- Sort(orderBy=[i_class_id ASC])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(inv_item_sk_node_9, i_class_id)], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name, 6Qxhc, inv_item_sk_node_9, inv_warehouse_sk_node_9, inv_quantity_on_hand_node_9], build=[right])
                  :- SortLimit(orderBy=[i_manufact ASC], offset=[0], fetch=[15], global=[true])
                  :  +- Exchange(distribution=[single])
                  :     +- SortLimit(orderBy=[i_manufact ASC], offset=[0], fetch=[15], global=[false])
                  :        +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
                  +- Exchange(distribution=[broadcast])
                     +- Calc(select=[inv_date_sk AS 6Qxhc, inv_item_sk AS inv_item_sk_node_9, inv_warehouse_sk AS inv_warehouse_sk_node_9, inv_quantity_on_hand AS inv_quantity_on_hand_node_9])
                        +- SortLimit(orderBy=[inv_quantity_on_hand ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[inv_quantity_on_hand ASC], offset=[0], fetch=[1], global=[false])
                                 +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS i_item_sk_node_8, 'hello' AS _c1, 'hello' AS _c2])
+- SortAggregate(isMerge=[true], groupBy=[i_class_id], select=[i_class_id, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[i_class_id ASC])
         +- Exchange(distribution=[hash[i_class_id]])
            +- LocalSortAggregate(groupBy=[i_class_id], select=[i_class_id, Partial_MIN(i_item_sk) AS min$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[i_class_id ASC])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(inv_item_sk_node_9 = i_class_id)], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name, 6Qxhc, inv_item_sk_node_9, inv_warehouse_sk_node_9, inv_quantity_on_hand_node_9], build=[right])
                        :- SortLimit(orderBy=[i_manufact ASC], offset=[0], fetch=[15], global=[true])
                        :  +- Exchange(distribution=[single])
                        :     +- SortLimit(orderBy=[i_manufact ASC], offset=[0], fetch=[15], global=[false])
                        :        +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[inv_date_sk AS 6Qxhc, inv_item_sk AS inv_item_sk_node_9, inv_warehouse_sk AS inv_warehouse_sk_node_9, inv_quantity_on_hand AS inv_quantity_on_hand_node_9])
                              +- SortLimit(orderBy=[inv_quantity_on_hand ASC], offset=[0], fetch=[1], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- SortLimit(orderBy=[inv_quantity_on_hand ASC], offset=[0], fetch=[1], global=[false])
                                       +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0