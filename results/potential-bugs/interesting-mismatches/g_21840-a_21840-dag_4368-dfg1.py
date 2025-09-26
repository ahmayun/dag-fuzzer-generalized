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
    return values.max() - values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_9 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_8 = autonode_10.add_columns(lit("hello"))
autonode_7 = autonode_9.order_by(col('d_year_node_9'))
autonode_6 = autonode_8.group_by(col('t_am_pm_node_10')).select(col('t_minute_node_10').max.alias('t_minute_node_10'))
autonode_5 = autonode_7.order_by(col('d_quarter_name_node_9'))
autonode_4 = autonode_5.join(autonode_6, col('d_fy_year_node_9') == col('t_minute_node_10'))
autonode_3 = autonode_4.filter(col('d_fy_year_node_9') > 48)
autonode_2 = autonode_3.group_by(col('d_fy_week_seq_node_9')).select(col('d_month_seq_node_9').min.alias('d_month_seq_node_9'))
autonode_1 = autonode_2.limit(35)
sink = autonode_1.group_by(col('d_month_seq_node_9')).select(col('d_month_seq_node_9').count.alias('d_month_seq_node_9'))
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
LogicalProject(d_month_seq_node_9=[$1])
+- LogicalAggregate(group=[{0}], EXPR$0=[COUNT($0)])
   +- LogicalSort(fetch=[35])
      +- LogicalProject(d_month_seq_node_9=[$1])
         +- LogicalAggregate(group=[{13}], EXPR$0=[MIN($3)])
            +- LogicalFilter(condition=[>($11, 48)])
               +- LogicalJoin(condition=[=($11, $28)], joinType=[inner])
                  :- LogicalSort(sort0=[$15], dir0=[ASC])
                  :  +- LogicalSort(sort0=[$6], dir0=[ASC])
                  :     +- LogicalProject(d_date_sk_node_9=[$0], d_date_id_node_9=[$1], d_date_node_9=[$2], d_month_seq_node_9=[$3], d_week_seq_node_9=[$4], d_quarter_seq_node_9=[$5], d_year_node_9=[$6], d_dow_node_9=[$7], d_moy_node_9=[$8], d_dom_node_9=[$9], d_qoy_node_9=[$10], d_fy_year_node_9=[$11], d_fy_quarter_seq_node_9=[$12], d_fy_week_seq_node_9=[$13], d_day_name_node_9=[$14], d_quarter_name_node_9=[$15], d_holiday_node_9=[$16], d_weekend_node_9=[$17], d_following_holiday_node_9=[$18], d_first_dom_node_9=[$19], d_last_dom_node_9=[$20], d_same_day_ly_node_9=[$21], d_same_day_lq_node_9=[$22], d_current_day_node_9=[$23], d_current_week_node_9=[$24], d_current_month_node_9=[$25], d_current_quarter_node_9=[$26], d_current_year_node_9=[$27])
                  :        +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
                  +- LogicalProject(t_minute_node_10=[$1])
                     +- LogicalAggregate(group=[{1}], EXPR$0=[MAX($0)])
                        +- LogicalProject(t_minute_node_10=[$4], t_am_pm_node_10=[$6])
                           +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS d_month_seq_node_9])
+- HashAggregate(isMerge=[true], groupBy=[d_month_seq_node_9], select=[d_month_seq_node_9, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[hash[d_month_seq_node_9]])
      +- LocalHashAggregate(groupBy=[d_month_seq_node_9], select=[d_month_seq_node_9, Partial_COUNT(d_month_seq_node_9) AS count$0])
         +- Calc(select=[EXPR$0 AS d_month_seq_node_9])
            +- Limit(offset=[0], fetch=[35], global=[true])
               +- Exchange(distribution=[single])
                  +- Limit(offset=[0], fetch=[35], global=[false])
                     +- HashAggregate(isMerge=[true], groupBy=[d_fy_week_seq], select=[d_fy_week_seq, Final_MIN(min$0) AS EXPR$0])
                        +- Exchange(distribution=[hash[d_fy_week_seq]])
                           +- LocalHashAggregate(groupBy=[d_fy_week_seq], select=[d_fy_week_seq, Partial_MIN(d_month_seq) AS min$0])
                              +- HashJoin(joinType=[InnerJoin], where=[=(d_fy_year, EXPR$0)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, EXPR$0], isBroadcast=[true], build=[right])
                                 :- Calc(select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year], where=[>(d_fy_year, 48)])
                                 :  +- Sort(orderBy=[d_quarter_name ASC])
                                 :     +- Sort(orderBy=[d_year ASC])
                                 :        +- Exchange(distribution=[single])
                                 :           +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
                                 +- Exchange(distribution=[broadcast])
                                    +- Calc(select=[EXPR$0], where=[>(EXPR$0, 48)])
                                       +- HashAggregate(isMerge=[true], groupBy=[t_am_pm], select=[t_am_pm, Final_MAX(max$0) AS EXPR$0])
                                          +- Exchange(distribution=[hash[t_am_pm]])
                                             +- LocalHashAggregate(groupBy=[t_am_pm], select=[t_am_pm, Partial_MAX(t_minute) AS max$0])
                                                +- TableSourceScan(table=[[default_catalog, default_database, time_dim, project=[t_minute, t_am_pm], metadata=[]]], fields=[t_minute, t_am_pm])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS d_month_seq_node_9])
+- HashAggregate(isMerge=[true], groupBy=[d_month_seq_node_9], select=[d_month_seq_node_9, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[hash[d_month_seq_node_9]])
      +- LocalHashAggregate(groupBy=[d_month_seq_node_9], select=[d_month_seq_node_9, Partial_COUNT(d_month_seq_node_9) AS count$0])
         +- Calc(select=[EXPR$0 AS d_month_seq_node_9])
            +- Limit(offset=[0], fetch=[35], global=[true])
               +- Exchange(distribution=[single])
                  +- Limit(offset=[0], fetch=[35], global=[false])
                     +- HashAggregate(isMerge=[true], groupBy=[d_fy_week_seq], select=[d_fy_week_seq, Final_MIN(min$0) AS EXPR$0])
                        +- Exchange(distribution=[hash[d_fy_week_seq]])
                           +- LocalHashAggregate(groupBy=[d_fy_week_seq], select=[d_fy_week_seq, Partial_MIN(d_month_seq) AS min$0])
                              +- HashJoin(joinType=[InnerJoin], where=[(d_fy_year = EXPR$0)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, EXPR$0], isBroadcast=[true], build=[right])
                                 :- Calc(select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year], where=[(d_fy_year > 48)])
                                 :  +- Sort(orderBy=[d_quarter_name ASC])
                                 :     +- Sort(orderBy=[d_year ASC])
                                 :        +- Exchange(distribution=[single])
                                 :           +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
                                 +- Exchange(distribution=[broadcast])
                                    +- Calc(select=[EXPR$0], where=[(EXPR$0 > 48)])
                                       +- HashAggregate(isMerge=[true], groupBy=[t_am_pm], select=[t_am_pm, Final_MAX(max$0) AS EXPR$0])
                                          +- Exchange(distribution=[hash[t_am_pm]])
                                             +- LocalHashAggregate(groupBy=[t_am_pm], select=[t_am_pm, Partial_MAX(t_minute) AS max$0])
                                                +- TableSourceScan(table=[[default_catalog, default_database, time_dim, project=[t_minute, t_am_pm], metadata=[]]], fields=[t_minute, t_am_pm])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o12072707.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#23709442:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[15](input=RelSubset#23709440,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[15]), rel#23709439:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[15](input=RelSubset#23709438,groupBy=d_fy_year, d_fy_week_seq,select=d_fy_year, d_fy_week_seq, Partial_MIN(d_month_seq) AS min$0)]
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