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
    return values.quantile(0.25)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_8 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_7 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_5 = autonode_8.order_by(col('ss_ext_tax_node_8'))
autonode_4 = autonode_6.join(autonode_7, col('d_current_week_node_6') == col('d_current_year_node_7'))
autonode_3 = autonode_4.join(autonode_5, col('ss_sold_date_sk_node_8') == col('d_week_seq_node_6'))
autonode_2 = autonode_3.group_by(col('d_weekend_node_6')).select(col('d_moy_node_6').min.alias('d_moy_node_6'))
autonode_1 = autonode_2.alias('fEYLG')
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
      "error_message": "An error occurred while calling o317882914.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#640744269:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[18](input=RelSubset#640744267,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[18]), rel#640744266:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[18](input=RelSubset#640744265,groupBy=ss_sold_date_sk,select=ss_sold_date_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (18) must be less than size (1)
\tat org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkElementIndex(Preconditions.java:1371)
\tat org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkElementIndex(Preconditions.java:1353)
\tat org.apache.flink.calcite.shaded.com.google.common.collect.SingletonImmutableList.get(SingletonImmutableList.java:44)
\tat org.apache.calcite.util.Util$TransformingList.get(Util.java:2794)
\tat scala.collection.convert.Wrappers$JListWrapper.apply(Wrappers.scala:100)
\tat org.apache.flink.table.planner.plan.utils.RelExplainUtil$.$anonfun$collationToString$1(RelExplainUtil.scala:83)
\tat scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
\tat scala.collection.Iterator.foreach(Iterator.scala:943)
\tat scala.collection.Iterator.foreach$(Iterator.scala:943)
\tat scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
\tat scala.collection.IterableLike.foreach(IterableLike.scala:74)
\tat scala.collection.IterableLike.foreach$(IterableLike.scala:73)
\tat scala.collection.AbstractIterable.foreach(Iterable.scala:56)
\tat scala.collection.TraversableLike.map(TraversableLike.scala:286)
\tat scala.collection.TraversableLike.map$(TraversableLike.scala:279)
\tat scala.collection.AbstractTraversable.map(Traversable.scala:108)
\tat org.apache.flink.table.planner.plan.utils.RelExplainUtil$.collationToString(RelExplainUtil.scala:83)
\tat org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSort.explainTerms(BatchPhysicalSort.scala:61)
\tat org.apache.calcite.rel.AbstractRelNode.getDigestItems(AbstractRelNode.java:414)
\tat org.apache.calcite.rel.AbstractRelNode.deepHashCode(AbstractRelNode.java:396)
\tat org.apache.calcite.rel.AbstractRelNode$InnerRelDigest.hashCode(AbstractRelNode.java:448)
\tat java.base/java.util.HashMap.hash(HashMap.java:340)
\tat java.base/java.util.HashMap.get(HashMap.java:553)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.registerImpl(VolcanoPlanner.java:1289)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.register(VolcanoPlanner.java:598)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:613)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.transformTo(VolcanoRuleCall.java:144)
\t... 45 more
",
      "stdout": "",
      "stderr": ""
    },
    "unopt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalProject(fEYLG=[AS($1, _UTF-16LE'fEYLG')], _c1=[_UTF-16LE'hello'])
+- LogicalAggregate(group=[{17}], EXPR$0=[MIN($8)])
   +- LogicalJoin(condition=[=($56, $4)], joinType=[inner])
      :- LogicalJoin(condition=[=($24, $55)], joinType=[inner])
      :  :- LogicalProject(d_date_sk_node_6=[$0], d_date_id_node_6=[$1], d_date_node_6=[$2], d_month_seq_node_6=[$3], d_week_seq_node_6=[$4], d_quarter_seq_node_6=[$5], d_year_node_6=[$6], d_dow_node_6=[$7], d_moy_node_6=[$8], d_dom_node_6=[$9], d_qoy_node_6=[$10], d_fy_year_node_6=[$11], d_fy_quarter_seq_node_6=[$12], d_fy_week_seq_node_6=[$13], d_day_name_node_6=[$14], d_quarter_name_node_6=[$15], d_holiday_node_6=[$16], d_weekend_node_6=[$17], d_following_holiday_node_6=[$18], d_first_dom_node_6=[$19], d_last_dom_node_6=[$20], d_same_day_ly_node_6=[$21], d_same_day_lq_node_6=[$22], d_current_day_node_6=[$23], d_current_week_node_6=[$24], d_current_month_node_6=[$25], d_current_quarter_node_6=[$26], d_current_year_node_6=[$27])
      :  :  +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
      :  +- LogicalProject(d_date_sk_node_7=[$0], d_date_id_node_7=[$1], d_date_node_7=[$2], d_month_seq_node_7=[$3], d_week_seq_node_7=[$4], d_quarter_seq_node_7=[$5], d_year_node_7=[$6], d_dow_node_7=[$7], d_moy_node_7=[$8], d_dom_node_7=[$9], d_qoy_node_7=[$10], d_fy_year_node_7=[$11], d_fy_quarter_seq_node_7=[$12], d_fy_week_seq_node_7=[$13], d_day_name_node_7=[$14], d_quarter_name_node_7=[$15], d_holiday_node_7=[$16], d_weekend_node_7=[$17], d_following_holiday_node_7=[$18], d_first_dom_node_7=[$19], d_last_dom_node_7=[$20], d_same_day_ly_node_7=[$21], d_same_day_lq_node_7=[$22], d_current_day_node_7=[$23], d_current_week_node_7=[$24], d_current_month_node_7=[$25], d_current_quarter_node_7=[$26], d_current_year_node_7=[$27])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
      +- LogicalSort(sort0=[$18], dir0=[ASC])
         +- LogicalProject(ss_sold_date_sk_node_8=[$0], ss_sold_time_sk_node_8=[$1], ss_item_sk_node_8=[$2], ss_customer_sk_node_8=[$3], ss_cdemo_sk_node_8=[$4], ss_hdemo_sk_node_8=[$5], ss_addr_sk_node_8=[$6], ss_store_sk_node_8=[$7], ss_promo_sk_node_8=[$8], ss_ticket_number_node_8=[$9], ss_quantity_node_8=[$10], ss_wholesale_cost_node_8=[$11], ss_list_price_node_8=[$12], ss_sales_price_node_8=[$13], ss_ext_discount_amt_node_8=[$14], ss_ext_sales_price_node_8=[$15], ss_ext_wholesale_cost_node_8=[$16], ss_ext_list_price_node_8=[$17], ss_ext_tax_node_8=[$18], ss_coupon_amt_node_8=[$19], ss_net_paid_node_8=[$20], ss_net_paid_inc_tax_node_8=[$21], ss_net_profit_node_8=[$22])
            +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS fEYLG, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[d_weekend], select=[d_weekend, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[d_weekend]])
      +- LocalHashAggregate(groupBy=[d_weekend], select=[d_weekend, Partial_MIN(d_moy) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_sold_date_sk, d_week_seq)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, d_date_sk0, d_date_id0, d_date0, d_month_seq0, d_week_seq0, d_quarter_seq0, d_year0, d_dow0, d_moy0, d_dom0, d_qoy0, d_fy_year0, d_fy_quarter_seq0, d_fy_week_seq0, d_day_name0, d_quarter_name0, d_holiday0, d_weekend0, d_following_holiday0, d_first_dom0, d_last_dom0, d_same_day_ly0, d_same_day_lq0, d_current_day0, d_current_week0, d_current_month0, d_current_quarter0, d_current_year0, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[right])
            :- HashJoin(joinType=[InnerJoin], where=[=(d_current_week, d_current_year0)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, d_date_sk0, d_date_id0, d_date0, d_month_seq0, d_week_seq0, d_quarter_seq0, d_year0, d_dow0, d_moy0, d_dom0, d_qoy0, d_fy_year0, d_fy_quarter_seq0, d_fy_week_seq0, d_day_name0, d_quarter_name0, d_holiday0, d_weekend0, d_following_holiday0, d_first_dom0, d_last_dom0, d_same_day_ly0, d_same_day_lq0, d_current_day0, d_current_week0, d_current_month0, d_current_quarter0, d_current_year0], build=[left])
            :  :- Exchange(distribution=[hash[d_current_week]])
            :  :  +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
            :  +- Exchange(distribution=[hash[d_current_year]])
            :     +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
            +- Exchange(distribution=[broadcast])
               +- SortLimit(orderBy=[ss_ext_tax ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[ss_ext_tax ASC], offset=[0], fetch=[1], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS fEYLG, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[d_weekend], select=[d_weekend, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[d_weekend]])
      +- LocalHashAggregate(groupBy=[d_weekend], select=[d_weekend, Partial_MIN(d_moy) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_sold_date_sk = d_week_seq)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, d_date_sk0, d_date_id0, d_date0, d_month_seq0, d_week_seq0, d_quarter_seq0, d_year0, d_dow0, d_moy0, d_dom0, d_qoy0, d_fy_year0, d_fy_quarter_seq0, d_fy_week_seq0, d_day_name0, d_quarter_name0, d_holiday0, d_weekend0, d_following_holiday0, d_first_dom0, d_last_dom0, d_same_day_ly0, d_same_day_lq0, d_current_day0, d_current_week0, d_current_month0, d_current_quarter0, d_current_year0, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[right])
            :- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(d_current_week = d_current_year0)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, d_date_sk0, d_date_id0, d_date0, d_month_seq0, d_week_seq0, d_quarter_seq0, d_year0, d_dow0, d_moy0, d_dom0, d_qoy0, d_fy_year0, d_fy_quarter_seq0, d_fy_week_seq0, d_day_name0, d_quarter_name0, d_holiday0, d_weekend0, d_following_holiday0, d_first_dom0, d_last_dom0, d_same_day_ly0, d_same_day_lq0, d_current_day0, d_current_week0, d_current_month0, d_current_quarter0, d_current_year0], build=[left])
            :  :- Exchange(distribution=[hash[d_current_week]])
            :  :  +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
            :  +- Exchange(distribution=[hash[d_current_year]])
            :     +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
            +- Exchange(distribution=[broadcast])
               +- SortLimit(orderBy=[ss_ext_tax ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[ss_ext_tax ASC], offset=[0], fetch=[1], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0