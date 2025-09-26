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
    import numpy as np
    return np.exp(np.log(values[values > 0]).mean()) if (values > 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_8 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_7 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_6 = autonode_8.filter(col('cp_description_node_8').char_length < 5)
autonode_5 = autonode_7.select(col('ss_customer_sk_node_7'))
autonode_4 = autonode_6.order_by(col('cp_department_node_8'))
autonode_3 = autonode_5.order_by(col('ss_customer_sk_node_7'))
autonode_2 = autonode_3.join(autonode_4, col('ss_customer_sk_node_7') == col('cp_catalog_page_sk_node_8'))
autonode_1 = autonode_2.group_by(col('ss_customer_sk_node_7')).select(col('cp_catalog_page_number_node_8').max.alias('cp_catalog_page_number_node_8'))
sink = autonode_1.alias('jaDho')
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
      "error_message": "An error occurred while calling o269385568.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#544607145:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[4](input=RelSubset#544607143,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[4]), rel#544607142:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[4](input=RelSubset#544607141,groupBy=cp_catalog_page_sk,select=cp_catalog_page_sk, Partial_MAX(cp_catalog_page_number) AS max$0)]
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
LogicalProject(jaDho=[AS($1, _UTF-16LE'jaDho')])
+- LogicalAggregate(group=[{0}], EXPR$0=[MAX($7)])
   +- LogicalJoin(condition=[=($0, $1)], joinType=[inner])
      :- LogicalSort(sort0=[$0], dir0=[ASC])
      :  +- LogicalProject(ss_customer_sk_node_7=[$3])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalSort(sort0=[$4], dir0=[ASC])
         +- LogicalFilter(condition=[<(CHAR_LENGTH($7), 5)])
            +- LogicalProject(cp_catalog_page_sk_node_8=[$0], cp_catalog_page_id_node_8=[$1], cp_start_date_sk_node_8=[$2], cp_end_date_sk_node_8=[$3], cp_department_node_8=[$4], cp_catalog_number_node_8=[$5], cp_catalog_page_number_node_8=[$6], cp_description_node_8=[$7], cp_type_node_8=[$8])
               +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS jaDho])
+- SortAggregate(isMerge=[false], groupBy=[ss_customer_sk], select=[ss_customer_sk, MAX(cp_catalog_page_number) AS EXPR$0])
   +- Sort(orderBy=[ss_customer_sk ASC])
      +- Exchange(distribution=[hash[ss_customer_sk]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_customer_sk, cp_catalog_page_sk)], select=[ss_customer_sk, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- SortLimit(orderBy=[ss_customer_sk ASC], offset=[0], fetch=[1], global=[true])
            :     +- Exchange(distribution=[single])
            :        +- SortLimit(orderBy=[ss_customer_sk ASC], offset=[0], fetch=[1], global=[false])
            :           +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_customer_sk], metadata=[]]], fields=[ss_customer_sk])
            +- SortLimit(orderBy=[cp_department ASC], offset=[0], fetch=[1], global=[true])
               +- Exchange(distribution=[single])
                  +- SortLimit(orderBy=[cp_department ASC], offset=[0], fetch=[1], global=[false])
                     +- Calc(select=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], where=[<(CHAR_LENGTH(cp_description), 5)])
                        +- TableSourceScan(table=[[default_catalog, default_database, catalog_page, filter=[<(CHAR_LENGTH(cp_description), 5)]]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS jaDho])
+- SortAggregate(isMerge=[false], groupBy=[ss_customer_sk], select=[ss_customer_sk, MAX(cp_catalog_page_number) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[ss_customer_sk ASC])
         +- Exchange(distribution=[hash[ss_customer_sk]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_customer_sk = cp_catalog_page_sk)], select=[ss_customer_sk, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- SortLimit(orderBy=[ss_customer_sk ASC], offset=[0], fetch=[1], global=[true])
               :     +- Exchange(distribution=[single])
               :        +- SortLimit(orderBy=[ss_customer_sk ASC], offset=[0], fetch=[1], global=[false])
               :           +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_customer_sk], metadata=[]]], fields=[ss_customer_sk])
               +- SortLimit(orderBy=[cp_department ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[cp_department ASC], offset=[0], fetch=[1], global=[false])
                        +- Calc(select=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], where=[(CHAR_LENGTH(cp_description) < 5)])
                           +- TableSourceScan(table=[[default_catalog, default_database, catalog_page, filter=[<(CHAR_LENGTH(cp_description), 5)]]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0