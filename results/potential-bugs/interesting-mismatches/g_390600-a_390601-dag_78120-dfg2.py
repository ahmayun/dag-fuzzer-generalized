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
    return values.median()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_9 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_11 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_7 = autonode_9.join(autonode_10, col('t_time_node_10') == col('wr_returning_cdemo_sk_node_9'))
autonode_8 = autonode_11.filter(col('i_rec_end_date_node_11').char_length <= 5)
autonode_5 = autonode_7.limit(63)
autonode_6 = autonode_8.distinct()
autonode_3 = autonode_5.order_by(col('wr_returned_date_sk_node_9'))
autonode_4 = autonode_6.order_by(col('i_brand_node_11'))
autonode_2 = autonode_3.join(autonode_4, col('wr_return_amt_node_9') == col('i_wholesale_cost_node_11'))
autonode_1 = autonode_2.group_by(col('wr_web_page_sk_node_9')).select(col('wr_web_page_sk_node_9').min.alias('wr_web_page_sk_node_9'))
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
      "error_message": "An error occurred while calling o212582772.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#430088751:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[8](input=RelSubset#430088749,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[8]), rel#430088748:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[8](input=RelSubset#430088747,groupBy=i_wholesale_cost,select=i_wholesale_cost)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (8) must be less than size (1)
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
LogicalProject(wr_web_page_sk_node_9=[$1], _c1=[_UTF-16LE'hello'])
+- LogicalAggregate(group=[{11}], EXPR$0=[MIN($11)])
   +- LogicalJoin(condition=[=($15, $40)], joinType=[inner])
      :- LogicalSort(sort0=[$0], dir0=[ASC])
      :  +- LogicalSort(fetch=[63])
      :     +- LogicalJoin(condition=[=($26, $8)], joinType=[inner])
      :        :- LogicalProject(wr_returned_date_sk_node_9=[$0], wr_returned_time_sk_node_9=[$1], wr_item_sk_node_9=[$2], wr_refunded_customer_sk_node_9=[$3], wr_refunded_cdemo_sk_node_9=[$4], wr_refunded_hdemo_sk_node_9=[$5], wr_refunded_addr_sk_node_9=[$6], wr_returning_customer_sk_node_9=[$7], wr_returning_cdemo_sk_node_9=[$8], wr_returning_hdemo_sk_node_9=[$9], wr_returning_addr_sk_node_9=[$10], wr_web_page_sk_node_9=[$11], wr_reason_sk_node_9=[$12], wr_order_number_node_9=[$13], wr_return_quantity_node_9=[$14], wr_return_amt_node_9=[$15], wr_return_tax_node_9=[$16], wr_return_amt_inc_tax_node_9=[$17], wr_fee_node_9=[$18], wr_return_ship_cost_node_9=[$19], wr_refunded_cash_node_9=[$20], wr_reversed_charge_node_9=[$21], wr_account_credit_node_9=[$22], wr_net_loss_node_9=[$23])
      :        :  +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
      :        +- LogicalProject(t_time_sk_node_10=[$0], t_time_id_node_10=[$1], t_time_node_10=[$2], t_hour_node_10=[$3], t_minute_node_10=[$4], t_second_node_10=[$5], t_am_pm_node_10=[$6], t_shift_node_10=[$7], t_sub_shift_node_10=[$8], t_meal_time_node_10=[$9])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
      +- LogicalSort(sort0=[$8], dir0=[ASC])
         +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}])
            +- LogicalFilter(condition=[<=(CHAR_LENGTH($3), 5)])
               +- LogicalProject(i_item_sk_node_11=[$0], i_item_id_node_11=[$1], i_rec_start_date_node_11=[$2], i_rec_end_date_node_11=[$3], i_item_desc_node_11=[$4], i_current_price_node_11=[$5], i_wholesale_cost_node_11=[$6], i_brand_id_node_11=[$7], i_brand_node_11=[$8], i_class_id_node_11=[$9], i_class_node_11=[$10], i_category_id_node_11=[$11], i_category_node_11=[$12], i_manufact_id_node_11=[$13], i_manufact_node_11=[$14], i_size_node_11=[$15], i_formulation_node_11=[$16], i_color_node_11=[$17], i_units_node_11=[$18], i_container_node_11=[$19], i_manager_id_node_11=[$20], i_product_name_node_11=[$21])
                  +- LogicalTableScan(table=[[default_catalog, default_database, item]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS wr_web_page_sk_node_9, 'hello' AS _c1])
+- SortAggregate(isMerge=[false], groupBy=[wr_web_page_sk], select=[wr_web_page_sk, MIN(wr_web_page_sk) AS EXPR$0])
   +- Sort(orderBy=[wr_web_page_sk ASC])
      +- Exchange(distribution=[hash[wr_web_page_sk]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(wr_return_amt, i_wholesale_cost)], select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- SortLimit(orderBy=[wr_returned_date_sk ASC], offset=[0], fetch=[1], global=[true])
            :     +- Exchange(distribution=[single])
            :        +- SortLimit(orderBy=[wr_returned_date_sk ASC], offset=[0], fetch=[1], global=[false])
            :           +- Limit(offset=[0], fetch=[63], global=[true])
            :              +- Exchange(distribution=[single])
            :                 +- Limit(offset=[0], fetch=[63], global=[false])
            :                    +- HashJoin(joinType=[InnerJoin], where=[=(t_time, wr_returning_cdemo_sk)], select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], build=[right])
            :                       :- Exchange(distribution=[hash[wr_returning_cdemo_sk]])
            :                       :  +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
            :                       +- Exchange(distribution=[hash[t_time]])
            :                          +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
            +- SortLimit(orderBy=[i_brand ASC], offset=[0], fetch=[1], global=[true])
               +- Exchange(distribution=[single])
                  +- SortLimit(orderBy=[i_brand ASC], offset=[0], fetch=[1], global=[false])
                     +- HashAggregate(isMerge=[false], groupBy=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
                        +- Exchange(distribution=[hash[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name]])
                           +- Calc(select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], where=[<=(CHAR_LENGTH(i_rec_end_date), 5)])
                              +- TableSourceScan(table=[[default_catalog, default_database, item, filter=[<=(CHAR_LENGTH(i_rec_end_date), 5)]]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS wr_web_page_sk_node_9, 'hello' AS _c1])
+- SortAggregate(isMerge=[false], groupBy=[wr_web_page_sk], select=[wr_web_page_sk, MIN(wr_web_page_sk) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[wr_web_page_sk ASC])
         +- Exchange(distribution=[hash[wr_web_page_sk]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(wr_return_amt = i_wholesale_cost)], select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- SortLimit(orderBy=[wr_returned_date_sk ASC], offset=[0], fetch=[1], global=[true])
               :     +- Exchange(distribution=[single])
               :        +- SortLimit(orderBy=[wr_returned_date_sk ASC], offset=[0], fetch=[1], global=[false])
               :           +- Limit(offset=[0], fetch=[63], global=[true])
               :              +- Exchange(distribution=[single])
               :                 +- Limit(offset=[0], fetch=[63], global=[false])
               :                    +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(t_time = wr_returning_cdemo_sk)], select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], build=[right])
               :                       :- Exchange(distribution=[hash[wr_returning_cdemo_sk]])
               :                       :  +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
               :                       +- Exchange(distribution=[hash[t_time]])
               :                          +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
               +- SortLimit(orderBy=[i_brand ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[i_brand ASC], offset=[0], fetch=[1], global=[false])
                        +- HashAggregate(isMerge=[false], groupBy=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
                           +- Exchange(distribution=[hash[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name]])
                              +- Calc(select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], where=[(CHAR_LENGTH(i_rec_end_date) <= 5)])
                                 +- TableSourceScan(table=[[default_catalog, default_database, item, filter=[<=(CHAR_LENGTH(i_rec_end_date), 5)]]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0