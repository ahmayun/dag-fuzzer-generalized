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

autonode_9 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_8 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_7 = autonode_9.add_columns(lit("hello"))
autonode_6 = autonode_8.alias('0JLjN')
autonode_5 = autonode_7.order_by(col('sr_refunded_cash_node_9'))
autonode_4 = autonode_6.limit(55)
autonode_3 = autonode_4.join(autonode_5, col('sr_return_quantity_node_9') == col('d_year_node_8'))
autonode_2 = autonode_3.alias('ww39O')
autonode_1 = autonode_2.filter(col('d_current_quarter_node_8').char_length >= 5)
sink = autonode_1.group_by(col('sr_return_amt_node_9')).select(col('sr_return_amt_node_9').max.alias('sr_return_amt_node_9'))
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
LogicalProject(sr_return_amt_node_9=[$1])
+- LogicalAggregate(group=[{39}], EXPR$0=[MAX($39)])
   +- LogicalFilter(condition=[>=(CHAR_LENGTH($26), 5)])
      +- LogicalProject(ww39O=[AS($0, _UTF-16LE'ww39O')], d_date_id_node_8=[$1], d_date_node_8=[$2], d_month_seq_node_8=[$3], d_week_seq_node_8=[$4], d_quarter_seq_node_8=[$5], d_year_node_8=[$6], d_dow_node_8=[$7], d_moy_node_8=[$8], d_dom_node_8=[$9], d_qoy_node_8=[$10], d_fy_year_node_8=[$11], d_fy_quarter_seq_node_8=[$12], d_fy_week_seq_node_8=[$13], d_day_name_node_8=[$14], d_quarter_name_node_8=[$15], d_holiday_node_8=[$16], d_weekend_node_8=[$17], d_following_holiday_node_8=[$18], d_first_dom_node_8=[$19], d_last_dom_node_8=[$20], d_same_day_ly_node_8=[$21], d_same_day_lq_node_8=[$22], d_current_day_node_8=[$23], d_current_week_node_8=[$24], d_current_month_node_8=[$25], d_current_quarter_node_8=[$26], d_current_year_node_8=[$27], sr_returned_date_sk_node_9=[$28], sr_return_time_sk_node_9=[$29], sr_item_sk_node_9=[$30], sr_customer_sk_node_9=[$31], sr_cdemo_sk_node_9=[$32], sr_hdemo_sk_node_9=[$33], sr_addr_sk_node_9=[$34], sr_store_sk_node_9=[$35], sr_reason_sk_node_9=[$36], sr_ticket_number_node_9=[$37], sr_return_quantity_node_9=[$38], sr_return_amt_node_9=[$39], sr_return_tax_node_9=[$40], sr_return_amt_inc_tax_node_9=[$41], sr_fee_node_9=[$42], sr_return_ship_cost_node_9=[$43], sr_refunded_cash_node_9=[$44], sr_reversed_charge_node_9=[$45], sr_store_credit_node_9=[$46], sr_net_loss_node_9=[$47], _c20=[$48])
         +- LogicalJoin(condition=[=($38, $6)], joinType=[inner])
            :- LogicalSort(fetch=[55])
            :  +- LogicalProject(0JLjN=[AS($0, _UTF-16LE'0JLjN')], d_date_id_node_8=[$1], d_date_node_8=[$2], d_month_seq_node_8=[$3], d_week_seq_node_8=[$4], d_quarter_seq_node_8=[$5], d_year_node_8=[$6], d_dow_node_8=[$7], d_moy_node_8=[$8], d_dom_node_8=[$9], d_qoy_node_8=[$10], d_fy_year_node_8=[$11], d_fy_quarter_seq_node_8=[$12], d_fy_week_seq_node_8=[$13], d_day_name_node_8=[$14], d_quarter_name_node_8=[$15], d_holiday_node_8=[$16], d_weekend_node_8=[$17], d_following_holiday_node_8=[$18], d_first_dom_node_8=[$19], d_last_dom_node_8=[$20], d_same_day_ly_node_8=[$21], d_same_day_lq_node_8=[$22], d_current_day_node_8=[$23], d_current_week_node_8=[$24], d_current_month_node_8=[$25], d_current_quarter_node_8=[$26], d_current_year_node_8=[$27])
            :     +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
            +- LogicalSort(sort0=[$16], dir0=[ASC])
               +- LogicalProject(sr_returned_date_sk_node_9=[$0], sr_return_time_sk_node_9=[$1], sr_item_sk_node_9=[$2], sr_customer_sk_node_9=[$3], sr_cdemo_sk_node_9=[$4], sr_hdemo_sk_node_9=[$5], sr_addr_sk_node_9=[$6], sr_store_sk_node_9=[$7], sr_reason_sk_node_9=[$8], sr_ticket_number_node_9=[$9], sr_return_quantity_node_9=[$10], sr_return_amt_node_9=[$11], sr_return_tax_node_9=[$12], sr_return_amt_inc_tax_node_9=[$13], sr_fee_node_9=[$14], sr_return_ship_cost_node_9=[$15], sr_refunded_cash_node_9=[$16], sr_reversed_charge_node_9=[$17], sr_store_credit_node_9=[$18], sr_net_loss_node_9=[$19], _c20=[_UTF-16LE'hello'])
                  +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS sr_return_amt_node_9])
+- HashAggregate(isMerge=[false], groupBy=[sr_return_amt], select=[sr_return_amt, MAX(sr_return_amt) AS EXPR$0])
   +- Exchange(distribution=[hash[sr_return_amt]])
      +- HashJoin(joinType=[InnerJoin], where=[=(sr_return_quantity, d_year_node_8)], select=[0JLjN, d_date_id_node_8, d_date_node_8, d_month_seq_node_8, d_week_seq_node_8, d_quarter_seq_node_8, d_year_node_8, d_dow_node_8, d_moy_node_8, d_dom_node_8, d_qoy_node_8, d_fy_year_node_8, d_fy_quarter_seq_node_8, d_fy_week_seq_node_8, d_day_name_node_8, d_quarter_name_node_8, d_holiday_node_8, d_weekend_node_8, d_following_holiday_node_8, d_first_dom_node_8, d_last_dom_node_8, d_same_day_ly_node_8, d_same_day_lq_node_8, d_current_day_node_8, d_current_week_node_8, d_current_month_node_8, d_current_quarter_node_8, d_current_year_node_8, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], isBroadcast=[true], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[d_date_sk AS 0JLjN, d_date_id AS d_date_id_node_8, d_date AS d_date_node_8, d_month_seq AS d_month_seq_node_8, d_week_seq AS d_week_seq_node_8, d_quarter_seq AS d_quarter_seq_node_8, d_year AS d_year_node_8, d_dow AS d_dow_node_8, d_moy AS d_moy_node_8, d_dom AS d_dom_node_8, d_qoy AS d_qoy_node_8, d_fy_year AS d_fy_year_node_8, d_fy_quarter_seq AS d_fy_quarter_seq_node_8, d_fy_week_seq AS d_fy_week_seq_node_8, d_day_name AS d_day_name_node_8, d_quarter_name AS d_quarter_name_node_8, d_holiday AS d_holiday_node_8, d_weekend AS d_weekend_node_8, d_following_holiday AS d_following_holiday_node_8, d_first_dom AS d_first_dom_node_8, d_last_dom AS d_last_dom_node_8, d_same_day_ly AS d_same_day_ly_node_8, d_same_day_lq AS d_same_day_lq_node_8, d_current_day AS d_current_day_node_8, d_current_week AS d_current_week_node_8, d_current_month AS d_current_month_node_8, d_current_quarter AS d_current_quarter_node_8, d_current_year AS d_current_year_node_8], where=[>=(CHAR_LENGTH(d_current_quarter), 5)])
         :     +- Limit(offset=[0], fetch=[55], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- Limit(offset=[0], fetch=[55], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, date_dim, limit=[55]]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
         +- Sort(orderBy=[sr_refunded_cash ASC])
            +- Exchange(distribution=[single])
               +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS sr_return_amt_node_9])
+- HashAggregate(isMerge=[false], groupBy=[sr_return_amt], select=[sr_return_amt, MAX(sr_return_amt) AS EXPR$0])
   +- Exchange(distribution=[hash[sr_return_amt]])
      +- HashJoin(joinType=[InnerJoin], where=[(sr_return_quantity = d_year_node_8)], select=[0JLjN, d_date_id_node_8, d_date_node_8, d_month_seq_node_8, d_week_seq_node_8, d_quarter_seq_node_8, d_year_node_8, d_dow_node_8, d_moy_node_8, d_dom_node_8, d_qoy_node_8, d_fy_year_node_8, d_fy_quarter_seq_node_8, d_fy_week_seq_node_8, d_day_name_node_8, d_quarter_name_node_8, d_holiday_node_8, d_weekend_node_8, d_following_holiday_node_8, d_first_dom_node_8, d_last_dom_node_8, d_same_day_ly_node_8, d_same_day_lq_node_8, d_current_day_node_8, d_current_week_node_8, d_current_month_node_8, d_current_quarter_node_8, d_current_year_node_8, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], isBroadcast=[true], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[d_date_sk AS 0JLjN, d_date_id AS d_date_id_node_8, d_date AS d_date_node_8, d_month_seq AS d_month_seq_node_8, d_week_seq AS d_week_seq_node_8, d_quarter_seq AS d_quarter_seq_node_8, d_year AS d_year_node_8, d_dow AS d_dow_node_8, d_moy AS d_moy_node_8, d_dom AS d_dom_node_8, d_qoy AS d_qoy_node_8, d_fy_year AS d_fy_year_node_8, d_fy_quarter_seq AS d_fy_quarter_seq_node_8, d_fy_week_seq AS d_fy_week_seq_node_8, d_day_name AS d_day_name_node_8, d_quarter_name AS d_quarter_name_node_8, d_holiday AS d_holiday_node_8, d_weekend AS d_weekend_node_8, d_following_holiday AS d_following_holiday_node_8, d_first_dom AS d_first_dom_node_8, d_last_dom AS d_last_dom_node_8, d_same_day_ly AS d_same_day_ly_node_8, d_same_day_lq AS d_same_day_lq_node_8, d_current_day AS d_current_day_node_8, d_current_week AS d_current_week_node_8, d_current_month AS d_current_month_node_8, d_current_quarter AS d_current_quarter_node_8, d_current_year AS d_current_year_node_8], where=[(CHAR_LENGTH(d_current_quarter) >= 5)])
         :     +- Limit(offset=[0], fetch=[55], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- Limit(offset=[0], fetch=[55], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, date_dim, limit=[55]]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
         +- Sort(orderBy=[sr_refunded_cash ASC])
            +- Exchange(distribution=[single])
               +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o231703413.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#468558314:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[16](input=RelSubset#468558312,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[16]), rel#468558311:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[16](input=RelSubset#468558310,groupBy=sr_return_quantity, sr_return_amt,select=sr_return_quantity, sr_return_amt, Partial_MAX(sr_return_amt) AS max$0)]
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