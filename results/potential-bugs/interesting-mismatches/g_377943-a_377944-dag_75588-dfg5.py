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
    return values.count()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_7 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_4 = autonode_6.filter(preloaded_udf_boolean(col('cd_dep_employed_count_node_6')))
autonode_5 = autonode_7.group_by(col('t_meal_time_node_7')).select(col('t_hour_node_7').min.alias('t_hour_node_7'))
autonode_2 = autonode_4.order_by(col('cd_dep_count_node_6'))
autonode_3 = autonode_5.distinct()
autonode_1 = autonode_2.join(autonode_3, col('t_hour_node_7') == col('cd_dep_employed_count_node_6'))
sink = autonode_1.group_by(col('cd_gender_node_6')).select(col('cd_dep_count_node_6').min.alias('cd_dep_count_node_6'))
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
LogicalProject(cd_dep_count_node_6=[$1])
+- LogicalAggregate(group=[{1}], EXPR$0=[MIN($6)])
   +- LogicalJoin(condition=[=($9, $7)], joinType=[inner])
      :- LogicalSort(sort0=[$6], dir0=[ASC])
      :  +- LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($7)])
      :     +- LogicalProject(cd_demo_sk_node_6=[$0], cd_gender_node_6=[$1], cd_marital_status_node_6=[$2], cd_education_status_node_6=[$3], cd_purchase_estimate_node_6=[$4], cd_credit_rating_node_6=[$5], cd_dep_count_node_6=[$6], cd_dep_employed_count_node_6=[$7], cd_dep_college_count_node_6=[$8])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
      +- LogicalAggregate(group=[{0}])
         +- LogicalProject(t_hour_node_7=[$1])
            +- LogicalAggregate(group=[{1}], EXPR$0=[MIN($0)])
               +- LogicalProject(t_hour_node_7=[$3], t_meal_time_node_7=[$9])
                  +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cd_dep_count_node_6])
+- HashAggregate(isMerge=[true], groupBy=[cd_gender], select=[cd_gender, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cd_gender]])
      +- LocalHashAggregate(groupBy=[cd_gender], select=[cd_gender, Partial_MIN(cd_dep_count) AS min$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(t_hour_node_7, cd_dep_employed_count)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, t_hour_node_7], isBroadcast=[true], build=[right])
            :- Sort(orderBy=[cd_dep_count ASC])
            :  +- Exchange(distribution=[single])
            :     +- Calc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[f0])
            :        +- PythonCalc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(cd_dep_employed_count) AS f0])
            :           +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
            +- Exchange(distribution=[broadcast])
               +- HashAggregate(isMerge=[true], groupBy=[t_hour_node_7], select=[t_hour_node_7])
                  +- Exchange(distribution=[hash[t_hour_node_7]])
                     +- LocalHashAggregate(groupBy=[t_hour_node_7], select=[t_hour_node_7])
                        +- Calc(select=[EXPR$0 AS t_hour_node_7])
                           +- HashAggregate(isMerge=[true], groupBy=[t_meal_time], select=[t_meal_time, Final_MIN(min$0) AS EXPR$0])
                              +- Exchange(distribution=[hash[t_meal_time]])
                                 +- LocalHashAggregate(groupBy=[t_meal_time], select=[t_meal_time, Partial_MIN(t_hour) AS min$0])
                                    +- TableSourceScan(table=[[default_catalog, default_database, time_dim, project=[t_hour, t_meal_time], metadata=[]]], fields=[t_hour, t_meal_time])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cd_dep_count_node_6])
+- HashAggregate(isMerge=[true], groupBy=[cd_gender], select=[cd_gender, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cd_gender]])
      +- LocalHashAggregate(groupBy=[cd_gender], select=[cd_gender, Partial_MIN(cd_dep_count) AS min$0])
         +- HashJoin(joinType=[InnerJoin], where=[(t_hour_node_7 = cd_dep_employed_count)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, t_hour_node_7], isBroadcast=[true], build=[right])
            :- Sort(orderBy=[cd_dep_count ASC])
            :  +- Exchange(distribution=[single])
            :     +- Calc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[f0])
            :        +- PythonCalc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(cd_dep_employed_count) AS f0])
            :           +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
            +- Exchange(distribution=[broadcast])
               +- HashAggregate(isMerge=[true], groupBy=[t_hour_node_7], select=[t_hour_node_7])
                  +- Exchange(distribution=[hash[t_hour_node_7]])
                     +- LocalHashAggregate(groupBy=[t_hour_node_7], select=[t_hour_node_7])
                        +- Calc(select=[EXPR$0 AS t_hour_node_7])
                           +- HashAggregate(isMerge=[true], groupBy=[t_meal_time], select=[t_meal_time, Final_MIN(min$0) AS EXPR$0])
                              +- Exchange(distribution=[hash[t_meal_time]])
                                 +- LocalHashAggregate(groupBy=[t_meal_time], select=[t_meal_time, Partial_MIN(t_hour) AS min$0])
                                    +- TableSourceScan(table=[[default_catalog, default_database, time_dim, project=[t_hour, t_meal_time], metadata=[]]], fields=[t_hour, t_meal_time])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o205501277.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#416402849:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[6](input=RelSubset#416402847,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[6]), rel#416402846:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[6](input=RelSubset#416402845,groupBy=cd_gender, cd_dep_employed_count,select=cd_gender, cd_dep_employed_count, Partial_MIN(cd_dep_count) AS min$0)]
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