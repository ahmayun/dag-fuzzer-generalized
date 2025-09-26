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
    return values.max()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_7 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_4 = autonode_6.order_by(col('d_week_seq_node_6'))
autonode_5 = autonode_7.select(col('wr_returned_time_sk_node_7'))
autonode_3 = autonode_4.join(autonode_5, col('d_dom_node_6') == col('wr_returned_time_sk_node_7'))
autonode_2 = autonode_3.group_by(col('d_first_dom_node_6')).select(col('d_week_seq_node_6').min.alias('d_week_seq_node_6'))
autonode_1 = autonode_2.alias('lUSgG')
sink = autonode_1.limit(68)
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
LogicalSort(fetch=[68])
+- LogicalProject(lUSgG=[AS($1, _UTF-16LE'lUSgG')])
   +- LogicalAggregate(group=[{19}], EXPR$0=[MIN($4)])
      +- LogicalJoin(condition=[=($9, $28)], joinType=[inner])
         :- LogicalSort(sort0=[$4], dir0=[ASC])
         :  +- LogicalProject(d_date_sk_node_6=[$0], d_date_id_node_6=[$1], d_date_node_6=[$2], d_month_seq_node_6=[$3], d_week_seq_node_6=[$4], d_quarter_seq_node_6=[$5], d_year_node_6=[$6], d_dow_node_6=[$7], d_moy_node_6=[$8], d_dom_node_6=[$9], d_qoy_node_6=[$10], d_fy_year_node_6=[$11], d_fy_quarter_seq_node_6=[$12], d_fy_week_seq_node_6=[$13], d_day_name_node_6=[$14], d_quarter_name_node_6=[$15], d_holiday_node_6=[$16], d_weekend_node_6=[$17], d_following_holiday_node_6=[$18], d_first_dom_node_6=[$19], d_last_dom_node_6=[$20], d_same_day_ly_node_6=[$21], d_same_day_lq_node_6=[$22], d_current_day_node_6=[$23], d_current_week_node_6=[$24], d_current_month_node_6=[$25], d_current_quarter_node_6=[$26], d_current_year_node_6=[$27])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
         +- LogicalProject(wr_returned_time_sk_node_7=[$1])
            +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS lUSgG])
+- Limit(offset=[0], fetch=[68], global=[true])
   +- Exchange(distribution=[single])
      +- Limit(offset=[0], fetch=[68], global=[false])
         +- HashAggregate(isMerge=[true], groupBy=[d_first_dom], select=[d_first_dom, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[hash[d_first_dom]])
               +- LocalHashAggregate(groupBy=[d_first_dom], select=[d_first_dom, Partial_MIN(d_week_seq) AS min$0])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(d_dom, wr_returned_time_sk)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, wr_returned_time_sk], build=[right])
                     :- Sort(orderBy=[d_week_seq ASC])
                     :  +- Exchange(distribution=[single])
                     :     +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
                     +- Exchange(distribution=[broadcast])
                        +- TableSourceScan(table=[[default_catalog, default_database, web_returns, project=[wr_returned_time_sk], metadata=[]]], fields=[wr_returned_time_sk])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS lUSgG])
+- Limit(offset=[0], fetch=[68], global=[true])
   +- Exchange(distribution=[single])
      +- Limit(offset=[0], fetch=[68], global=[false])
         +- HashAggregate(isMerge=[true], groupBy=[d_first_dom], select=[d_first_dom, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[hash[d_first_dom]])
               +- LocalHashAggregate(groupBy=[d_first_dom], select=[d_first_dom, Partial_MIN(d_week_seq) AS min$0])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[(d_dom = wr_returned_time_sk)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, wr_returned_time_sk], build=[right])
                     :- Sort(orderBy=[d_week_seq ASC])
                     :  +- Exchange(distribution=[single])
                     :     +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
                     +- Exchange(distribution=[broadcast])
                        +- TableSourceScan(table=[[default_catalog, default_database, web_returns, project=[wr_returned_time_sk], metadata=[]]], fields=[wr_returned_time_sk])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o149546870.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#301696271:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[4](input=RelSubset#301696269,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[4]), rel#301696268:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[4](input=RelSubset#301696267,groupBy=d_dom, d_first_dom,select=d_dom, d_first_dom, Partial_MIN(d_week_seq) AS min$0)]
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