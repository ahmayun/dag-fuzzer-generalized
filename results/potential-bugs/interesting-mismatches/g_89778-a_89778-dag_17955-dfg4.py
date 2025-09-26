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

autonode_6 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_5 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_4 = autonode_6.alias('RU8qb')
autonode_3 = autonode_5.order_by(col('cr_returning_customer_sk_node_5'))
autonode_2 = autonode_3.join(autonode_4, col('cr_returned_time_sk_node_5') == col('d_same_day_lq_node_6'))
autonode_1 = autonode_2.group_by(col('cr_returning_customer_sk_node_5')).select(col('d_month_seq_node_6').count.alias('d_month_seq_node_6'))
sink = autonode_1.filter(preloaded_udf_boolean(col('d_month_seq_node_6')))
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
LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($0)])
+- LogicalProject(d_month_seq_node_6=[$1])
   +- LogicalAggregate(group=[{7}], EXPR$0=[COUNT($30)])
      +- LogicalJoin(condition=[=($1, $49)], joinType=[inner])
         :- LogicalSort(sort0=[$7], dir0=[ASC])
         :  +- LogicalProject(cr_returned_date_sk_node_5=[$0], cr_returned_time_sk_node_5=[$1], cr_item_sk_node_5=[$2], cr_refunded_customer_sk_node_5=[$3], cr_refunded_cdemo_sk_node_5=[$4], cr_refunded_hdemo_sk_node_5=[$5], cr_refunded_addr_sk_node_5=[$6], cr_returning_customer_sk_node_5=[$7], cr_returning_cdemo_sk_node_5=[$8], cr_returning_hdemo_sk_node_5=[$9], cr_returning_addr_sk_node_5=[$10], cr_call_center_sk_node_5=[$11], cr_catalog_page_sk_node_5=[$12], cr_ship_mode_sk_node_5=[$13], cr_warehouse_sk_node_5=[$14], cr_reason_sk_node_5=[$15], cr_order_number_node_5=[$16], cr_return_quantity_node_5=[$17], cr_return_amount_node_5=[$18], cr_return_tax_node_5=[$19], cr_return_amt_inc_tax_node_5=[$20], cr_fee_node_5=[$21], cr_return_ship_cost_node_5=[$22], cr_refunded_cash_node_5=[$23], cr_reversed_charge_node_5=[$24], cr_store_credit_node_5=[$25], cr_net_loss_node_5=[$26])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
         +- LogicalProject(RU8qb=[AS($0, _UTF-16LE'RU8qb')], d_date_id_node_6=[$1], d_date_node_6=[$2], d_month_seq_node_6=[$3], d_week_seq_node_6=[$4], d_quarter_seq_node_6=[$5], d_year_node_6=[$6], d_dow_node_6=[$7], d_moy_node_6=[$8], d_dom_node_6=[$9], d_qoy_node_6=[$10], d_fy_year_node_6=[$11], d_fy_quarter_seq_node_6=[$12], d_fy_week_seq_node_6=[$13], d_day_name_node_6=[$14], d_quarter_name_node_6=[$15], d_holiday_node_6=[$16], d_weekend_node_6=[$17], d_following_holiday_node_6=[$18], d_first_dom_node_6=[$19], d_last_dom_node_6=[$20], d_same_day_ly_node_6=[$21], d_same_day_lq_node_6=[$22], d_current_day_node_6=[$23], d_current_week_node_6=[$24], d_current_month_node_6=[$25], d_current_quarter_node_6=[$26], d_current_year_node_6=[$27])
            +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0], where=[f0])
+- PythonCalc(select=[EXPR$0, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(EXPR$0) AS f0])
   +- HashAggregate(isMerge=[true], groupBy=[cr_returning_customer_sk], select=[cr_returning_customer_sk, Final_COUNT(count$0) AS EXPR$0])
      +- Exchange(distribution=[hash[cr_returning_customer_sk]])
         +- LocalHashAggregate(groupBy=[cr_returning_customer_sk], select=[cr_returning_customer_sk, Partial_COUNT(d_month_seq_node_6) AS count$0])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cr_returned_time_sk, d_same_day_lq_node_6)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, RU8qb, d_date_id_node_6, d_date_node_6, d_month_seq_node_6, d_week_seq_node_6, d_quarter_seq_node_6, d_year_node_6, d_dow_node_6, d_moy_node_6, d_dom_node_6, d_qoy_node_6, d_fy_year_node_6, d_fy_quarter_seq_node_6, d_fy_week_seq_node_6, d_day_name_node_6, d_quarter_name_node_6, d_holiday_node_6, d_weekend_node_6, d_following_holiday_node_6, d_first_dom_node_6, d_last_dom_node_6, d_same_day_ly_node_6, d_same_day_lq_node_6, d_current_day_node_6, d_current_week_node_6, d_current_month_node_6, d_current_quarter_node_6, d_current_year_node_6], build=[right])
               :- Sort(orderBy=[cr_returning_customer_sk ASC])
               :  +- Exchange(distribution=[single])
               :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
               +- Exchange(distribution=[broadcast])
                  +- Calc(select=[d_date_sk AS RU8qb, d_date_id AS d_date_id_node_6, d_date AS d_date_node_6, d_month_seq AS d_month_seq_node_6, d_week_seq AS d_week_seq_node_6, d_quarter_seq AS d_quarter_seq_node_6, d_year AS d_year_node_6, d_dow AS d_dow_node_6, d_moy AS d_moy_node_6, d_dom AS d_dom_node_6, d_qoy AS d_qoy_node_6, d_fy_year AS d_fy_year_node_6, d_fy_quarter_seq AS d_fy_quarter_seq_node_6, d_fy_week_seq AS d_fy_week_seq_node_6, d_day_name AS d_day_name_node_6, d_quarter_name AS d_quarter_name_node_6, d_holiday AS d_holiday_node_6, d_weekend AS d_weekend_node_6, d_following_holiday AS d_following_holiday_node_6, d_first_dom AS d_first_dom_node_6, d_last_dom AS d_last_dom_node_6, d_same_day_ly AS d_same_day_ly_node_6, d_same_day_lq AS d_same_day_lq_node_6, d_current_day AS d_current_day_node_6, d_current_week AS d_current_week_node_6, d_current_month AS d_current_month_node_6, d_current_quarter AS d_current_quarter_node_6, d_current_year AS d_current_year_node_6])
                     +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])

== Optimized Execution Plan ==
Calc(select=[EXPR$0], where=[f0])
+- PythonCalc(select=[EXPR$0, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(EXPR$0) AS f0])
   +- HashAggregate(isMerge=[true], groupBy=[cr_returning_customer_sk], select=[cr_returning_customer_sk, Final_COUNT(count$0) AS EXPR$0])
      +- Exchange(distribution=[hash[cr_returning_customer_sk]])
         +- LocalHashAggregate(groupBy=[cr_returning_customer_sk], select=[cr_returning_customer_sk, Partial_COUNT(d_month_seq_node_6) AS count$0])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(cr_returned_time_sk = d_same_day_lq_node_6)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, RU8qb, d_date_id_node_6, d_date_node_6, d_month_seq_node_6, d_week_seq_node_6, d_quarter_seq_node_6, d_year_node_6, d_dow_node_6, d_moy_node_6, d_dom_node_6, d_qoy_node_6, d_fy_year_node_6, d_fy_quarter_seq_node_6, d_fy_week_seq_node_6, d_day_name_node_6, d_quarter_name_node_6, d_holiday_node_6, d_weekend_node_6, d_following_holiday_node_6, d_first_dom_node_6, d_last_dom_node_6, d_same_day_ly_node_6, d_same_day_lq_node_6, d_current_day_node_6, d_current_week_node_6, d_current_month_node_6, d_current_quarter_node_6, d_current_year_node_6], build=[right])
               :- Sort(orderBy=[cr_returning_customer_sk ASC])
               :  +- Exchange(distribution=[single])
               :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
               +- Exchange(distribution=[broadcast])
                  +- Calc(select=[d_date_sk AS RU8qb, d_date_id AS d_date_id_node_6, d_date AS d_date_node_6, d_month_seq AS d_month_seq_node_6, d_week_seq AS d_week_seq_node_6, d_quarter_seq AS d_quarter_seq_node_6, d_year AS d_year_node_6, d_dow AS d_dow_node_6, d_moy AS d_moy_node_6, d_dom AS d_dom_node_6, d_qoy AS d_qoy_node_6, d_fy_year AS d_fy_year_node_6, d_fy_quarter_seq AS d_fy_quarter_seq_node_6, d_fy_week_seq AS d_fy_week_seq_node_6, d_day_name AS d_day_name_node_6, d_quarter_name AS d_quarter_name_node_6, d_holiday AS d_holiday_node_6, d_weekend AS d_weekend_node_6, d_following_holiday AS d_following_holiday_node_6, d_first_dom AS d_first_dom_node_6, d_last_dom AS d_last_dom_node_6, d_same_day_ly AS d_same_day_ly_node_6, d_same_day_lq AS d_same_day_lq_node_6, d_current_day AS d_current_day_node_6, d_current_week AS d_current_week_node_6, d_current_month AS d_current_month_node_6, d_current_quarter AS d_current_quarter_node_6, d_current_year AS d_current_year_node_6])
                     +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o49021933.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#97772197:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[7](input=RelSubset#97772195,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[7]), rel#97772194:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[7](input=RelSubset#97772193,groupBy=cr_returned_time_sk, cr_returning_customer_sk,select=cr_returned_time_sk, cr_returning_customer_sk, Partial_COUNT(*) AS count1$0)]
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