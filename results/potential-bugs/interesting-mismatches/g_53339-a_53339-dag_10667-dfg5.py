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
autonode_12 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_11 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_8 = autonode_10.alias('jLAQd')
autonode_9 = autonode_11.join(autonode_12, col('sr_refunded_cash_node_11') == col('i_wholesale_cost_node_12'))
autonode_6 = autonode_8.alias('2Q1th')
autonode_7 = autonode_9.limit(65)
autonode_4 = autonode_6.order_by(col('t_second_node_10'))
autonode_5 = autonode_7.alias('RgGm4')
autonode_2 = autonode_4.add_columns(lit("hello"))
autonode_3 = autonode_5.limit(7)
autonode_1 = autonode_2.join(autonode_3, col('t_am_pm_node_10') == col('i_item_id_node_12'))
sink = autonode_1.group_by(col('sr_hdemo_sk_node_11')).select(col('i_class_id_node_12').max.alias('i_class_id_node_12'))
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
      "error_message": "An error occurred while calling o29308917.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#57586354:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[5](input=RelSubset#57586352,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[5]), rel#57586351:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[5](input=RelSubset#57586350,groupBy=t_am_pm,select=t_am_pm)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (5) must be less than size (1)
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
LogicalProject(i_class_id_node_12=[$1])
+- LogicalAggregate(group=[{16}], EXPR$0=[MAX($40)])
   +- LogicalJoin(condition=[=($6, $32)], joinType=[inner])
      :- LogicalProject(2Q1th=[$0], t_time_id_node_10=[$1], t_time_node_10=[$2], t_hour_node_10=[$3], t_minute_node_10=[$4], t_second_node_10=[$5], t_am_pm_node_10=[$6], t_shift_node_10=[$7], t_sub_shift_node_10=[$8], t_meal_time_node_10=[$9], _c10=[_UTF-16LE'hello'])
      :  +- LogicalSort(sort0=[$5], dir0=[ASC])
      :     +- LogicalProject(2Q1th=[AS(AS($0, _UTF-16LE'jLAQd'), _UTF-16LE'2Q1th')], t_time_id_node_10=[$1], t_time_node_10=[$2], t_hour_node_10=[$3], t_minute_node_10=[$4], t_second_node_10=[$5], t_am_pm_node_10=[$6], t_shift_node_10=[$7], t_sub_shift_node_10=[$8], t_meal_time_node_10=[$9])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
      +- LogicalSort(fetch=[7])
         +- LogicalProject(RgGm4=[AS($0, _UTF-16LE'RgGm4')], sr_return_time_sk_node_11=[$1], sr_item_sk_node_11=[$2], sr_customer_sk_node_11=[$3], sr_cdemo_sk_node_11=[$4], sr_hdemo_sk_node_11=[$5], sr_addr_sk_node_11=[$6], sr_store_sk_node_11=[$7], sr_reason_sk_node_11=[$8], sr_ticket_number_node_11=[$9], sr_return_quantity_node_11=[$10], sr_return_amt_node_11=[$11], sr_return_tax_node_11=[$12], sr_return_amt_inc_tax_node_11=[$13], sr_fee_node_11=[$14], sr_return_ship_cost_node_11=[$15], sr_refunded_cash_node_11=[$16], sr_reversed_charge_node_11=[$17], sr_store_credit_node_11=[$18], sr_net_loss_node_11=[$19], i_item_sk_node_12=[$20], i_item_id_node_12=[$21], i_rec_start_date_node_12=[$22], i_rec_end_date_node_12=[$23], i_item_desc_node_12=[$24], i_current_price_node_12=[$25], i_wholesale_cost_node_12=[$26], i_brand_id_node_12=[$27], i_brand_node_12=[$28], i_class_id_node_12=[$29], i_class_node_12=[$30], i_category_id_node_12=[$31], i_category_node_12=[$32], i_manufact_id_node_12=[$33], i_manufact_node_12=[$34], i_size_node_12=[$35], i_formulation_node_12=[$36], i_color_node_12=[$37], i_units_node_12=[$38], i_container_node_12=[$39], i_manager_id_node_12=[$40], i_product_name_node_12=[$41])
            +- LogicalSort(fetch=[65])
               +- LogicalJoin(condition=[=($16, $26)], joinType=[inner])
                  :- LogicalProject(sr_returned_date_sk_node_11=[$0], sr_return_time_sk_node_11=[$1], sr_item_sk_node_11=[$2], sr_customer_sk_node_11=[$3], sr_cdemo_sk_node_11=[$4], sr_hdemo_sk_node_11=[$5], sr_addr_sk_node_11=[$6], sr_store_sk_node_11=[$7], sr_reason_sk_node_11=[$8], sr_ticket_number_node_11=[$9], sr_return_quantity_node_11=[$10], sr_return_amt_node_11=[$11], sr_return_tax_node_11=[$12], sr_return_amt_inc_tax_node_11=[$13], sr_fee_node_11=[$14], sr_return_ship_cost_node_11=[$15], sr_refunded_cash_node_11=[$16], sr_reversed_charge_node_11=[$17], sr_store_credit_node_11=[$18], sr_net_loss_node_11=[$19])
                  :  +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
                  +- LogicalProject(i_item_sk_node_12=[$0], i_item_id_node_12=[$1], i_rec_start_date_node_12=[$2], i_rec_end_date_node_12=[$3], i_item_desc_node_12=[$4], i_current_price_node_12=[$5], i_wholesale_cost_node_12=[$6], i_brand_id_node_12=[$7], i_brand_node_12=[$8], i_class_id_node_12=[$9], i_class_node_12=[$10], i_category_id_node_12=[$11], i_category_node_12=[$12], i_manufact_id_node_12=[$13], i_manufact_node_12=[$14], i_size_node_12=[$15], i_formulation_node_12=[$16], i_color_node_12=[$17], i_units_node_12=[$18], i_container_node_12=[$19], i_manager_id_node_12=[$20], i_product_name_node_12=[$21])
                     +- LogicalTableScan(table=[[default_catalog, default_database, item]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS i_class_id_node_12])
+- SortAggregate(isMerge=[true], groupBy=[sr_hdemo_sk_node_11], select=[sr_hdemo_sk_node_11, Final_MAX(max$0) AS EXPR$0])
   +- Sort(orderBy=[sr_hdemo_sk_node_11 ASC])
      +- Exchange(distribution=[hash[sr_hdemo_sk_node_11]])
         +- LocalSortAggregate(groupBy=[sr_hdemo_sk_node_11], select=[sr_hdemo_sk_node_11, Partial_MAX(i_class_id_node_12) AS max$0])
            +- Sort(orderBy=[sr_hdemo_sk_node_11 ASC])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(t_am_pm_node_10, i_item_id_node_12)], select=[2Q1th, t_time_id_node_10, t_time_node_10, t_hour_node_10, t_minute_node_10, t_second_node_10, t_am_pm_node_10, t_shift_node_10, t_sub_shift_node_10, t_meal_time_node_10, _c10, RgGm4, sr_return_time_sk_node_11, sr_item_sk_node_11, sr_customer_sk_node_11, sr_cdemo_sk_node_11, sr_hdemo_sk_node_11, sr_addr_sk_node_11, sr_store_sk_node_11, sr_reason_sk_node_11, sr_ticket_number_node_11, sr_return_quantity_node_11, sr_return_amt_node_11, sr_return_tax_node_11, sr_return_amt_inc_tax_node_11, sr_fee_node_11, sr_return_ship_cost_node_11, sr_refunded_cash_node_11, sr_reversed_charge_node_11, sr_store_credit_node_11, sr_net_loss_node_11, i_item_sk_node_12, i_item_id_node_12, i_rec_start_date_node_12, i_rec_end_date_node_12, i_item_desc_node_12, i_current_price_node_12, i_wholesale_cost_node_12, i_brand_id_node_12, i_brand_node_12, i_class_id_node_12, i_class_node_12, i_category_id_node_12, i_category_node_12, i_manufact_id_node_12, i_manufact_node_12, i_size_node_12, i_formulation_node_12, i_color_node_12, i_units_node_12, i_container_node_12, i_manager_id_node_12, i_product_name_node_12], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[t_time_sk AS 2Q1th, t_time_id AS t_time_id_node_10, t_time AS t_time_node_10, t_hour AS t_hour_node_10, t_minute AS t_minute_node_10, t_second AS t_second_node_10, t_am_pm AS t_am_pm_node_10, t_shift AS t_shift_node_10, t_sub_shift AS t_sub_shift_node_10, t_meal_time AS t_meal_time_node_10, 'hello' AS _c10])
                  :     +- SortLimit(orderBy=[t_second ASC], offset=[0], fetch=[1], global=[true])
                  :        +- Exchange(distribution=[single])
                  :           +- SortLimit(orderBy=[t_second ASC], offset=[0], fetch=[1], global=[false])
                  :              +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
                  +- Calc(select=[sr_returned_date_sk AS RgGm4, sr_return_time_sk AS sr_return_time_sk_node_11, sr_item_sk AS sr_item_sk_node_11, sr_customer_sk AS sr_customer_sk_node_11, sr_cdemo_sk AS sr_cdemo_sk_node_11, sr_hdemo_sk AS sr_hdemo_sk_node_11, sr_addr_sk AS sr_addr_sk_node_11, sr_store_sk AS sr_store_sk_node_11, sr_reason_sk AS sr_reason_sk_node_11, sr_ticket_number AS sr_ticket_number_node_11, sr_return_quantity AS sr_return_quantity_node_11, sr_return_amt AS sr_return_amt_node_11, sr_return_tax AS sr_return_tax_node_11, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_11, sr_fee AS sr_fee_node_11, sr_return_ship_cost AS sr_return_ship_cost_node_11, sr_refunded_cash AS sr_refunded_cash_node_11, sr_reversed_charge AS sr_reversed_charge_node_11, sr_store_credit AS sr_store_credit_node_11, sr_net_loss AS sr_net_loss_node_11, i_item_sk AS i_item_sk_node_12, i_item_id AS i_item_id_node_12, i_rec_start_date AS i_rec_start_date_node_12, i_rec_end_date AS i_rec_end_date_node_12, i_item_desc AS i_item_desc_node_12, i_current_price AS i_current_price_node_12, i_wholesale_cost AS i_wholesale_cost_node_12, i_brand_id AS i_brand_id_node_12, i_brand AS i_brand_node_12, i_class_id AS i_class_id_node_12, i_class AS i_class_node_12, i_category_id AS i_category_id_node_12, i_category AS i_category_node_12, i_manufact_id AS i_manufact_id_node_12, i_manufact AS i_manufact_node_12, i_size AS i_size_node_12, i_formulation AS i_formulation_node_12, i_color AS i_color_node_12, i_units AS i_units_node_12, i_container AS i_container_node_12, i_manager_id AS i_manager_id_node_12, i_product_name AS i_product_name_node_12])
                     +- Limit(offset=[0], fetch=[7], global=[true])
                        +- Exchange(distribution=[single])
                           +- Limit(offset=[0], fetch=[7], global=[false])
                              +- Limit(offset=[0], fetch=[65], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- Limit(offset=[0], fetch=[65], global=[false])
                                       +- HashJoin(joinType=[InnerJoin], where=[=(sr_refunded_cash, i_wholesale_cost)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], build=[right])
                                          :- Exchange(distribution=[hash[sr_refunded_cash]])
                                          :  +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                                          +- Exchange(distribution=[hash[i_wholesale_cost]])
                                             +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS i_class_id_node_12])
+- SortAggregate(isMerge=[true], groupBy=[sr_hdemo_sk_node_11], select=[sr_hdemo_sk_node_11, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[sr_hdemo_sk_node_11 ASC])
         +- Exchange(distribution=[hash[sr_hdemo_sk_node_11]])
            +- LocalSortAggregate(groupBy=[sr_hdemo_sk_node_11], select=[sr_hdemo_sk_node_11, Partial_MAX(i_class_id_node_12) AS max$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[sr_hdemo_sk_node_11 ASC])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(t_am_pm_node_10 = i_item_id_node_12)], select=[2Q1th, t_time_id_node_10, t_time_node_10, t_hour_node_10, t_minute_node_10, t_second_node_10, t_am_pm_node_10, t_shift_node_10, t_sub_shift_node_10, t_meal_time_node_10, _c10, RgGm4, sr_return_time_sk_node_11, sr_item_sk_node_11, sr_customer_sk_node_11, sr_cdemo_sk_node_11, sr_hdemo_sk_node_11, sr_addr_sk_node_11, sr_store_sk_node_11, sr_reason_sk_node_11, sr_ticket_number_node_11, sr_return_quantity_node_11, sr_return_amt_node_11, sr_return_tax_node_11, sr_return_amt_inc_tax_node_11, sr_fee_node_11, sr_return_ship_cost_node_11, sr_refunded_cash_node_11, sr_reversed_charge_node_11, sr_store_credit_node_11, sr_net_loss_node_11, i_item_sk_node_12, i_item_id_node_12, i_rec_start_date_node_12, i_rec_end_date_node_12, i_item_desc_node_12, i_current_price_node_12, i_wholesale_cost_node_12, i_brand_id_node_12, i_brand_node_12, i_class_id_node_12, i_class_node_12, i_category_id_node_12, i_category_node_12, i_manufact_id_node_12, i_manufact_node_12, i_size_node_12, i_formulation_node_12, i_color_node_12, i_units_node_12, i_container_node_12, i_manager_id_node_12, i_product_name_node_12], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- Calc(select=[t_time_sk AS 2Q1th, t_time_id AS t_time_id_node_10, t_time AS t_time_node_10, t_hour AS t_hour_node_10, t_minute AS t_minute_node_10, t_second AS t_second_node_10, t_am_pm AS t_am_pm_node_10, t_shift AS t_shift_node_10, t_sub_shift AS t_sub_shift_node_10, t_meal_time AS t_meal_time_node_10, 'hello' AS _c10])
                        :     +- SortLimit(orderBy=[t_second ASC], offset=[0], fetch=[1], global=[true])
                        :        +- Exchange(distribution=[single])
                        :           +- SortLimit(orderBy=[t_second ASC], offset=[0], fetch=[1], global=[false])
                        :              +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
                        +- Calc(select=[sr_returned_date_sk AS RgGm4, sr_return_time_sk AS sr_return_time_sk_node_11, sr_item_sk AS sr_item_sk_node_11, sr_customer_sk AS sr_customer_sk_node_11, sr_cdemo_sk AS sr_cdemo_sk_node_11, sr_hdemo_sk AS sr_hdemo_sk_node_11, sr_addr_sk AS sr_addr_sk_node_11, sr_store_sk AS sr_store_sk_node_11, sr_reason_sk AS sr_reason_sk_node_11, sr_ticket_number AS sr_ticket_number_node_11, sr_return_quantity AS sr_return_quantity_node_11, sr_return_amt AS sr_return_amt_node_11, sr_return_tax AS sr_return_tax_node_11, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_11, sr_fee AS sr_fee_node_11, sr_return_ship_cost AS sr_return_ship_cost_node_11, sr_refunded_cash AS sr_refunded_cash_node_11, sr_reversed_charge AS sr_reversed_charge_node_11, sr_store_credit AS sr_store_credit_node_11, sr_net_loss AS sr_net_loss_node_11, i_item_sk AS i_item_sk_node_12, i_item_id AS i_item_id_node_12, i_rec_start_date AS i_rec_start_date_node_12, i_rec_end_date AS i_rec_end_date_node_12, i_item_desc AS i_item_desc_node_12, i_current_price AS i_current_price_node_12, i_wholesale_cost AS i_wholesale_cost_node_12, i_brand_id AS i_brand_id_node_12, i_brand AS i_brand_node_12, i_class_id AS i_class_id_node_12, i_class AS i_class_node_12, i_category_id AS i_category_id_node_12, i_category AS i_category_node_12, i_manufact_id AS i_manufact_id_node_12, i_manufact AS i_manufact_node_12, i_size AS i_size_node_12, i_formulation AS i_formulation_node_12, i_color AS i_color_node_12, i_units AS i_units_node_12, i_container AS i_container_node_12, i_manager_id AS i_manager_id_node_12, i_product_name AS i_product_name_node_12])
                           +- Limit(offset=[0], fetch=[7], global=[true])
                              +- Exchange(distribution=[single])
                                 +- Limit(offset=[0], fetch=[7], global=[false])
                                    +- Limit(offset=[0], fetch=[65], global=[true])
                                       +- Exchange(distribution=[single])
                                          +- Limit(offset=[0], fetch=[65], global=[false])
                                             +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(sr_refunded_cash = i_wholesale_cost)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], build=[right])
                                                :- Exchange(distribution=[hash[sr_refunded_cash]])
                                                :  +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                                                +- Exchange(distribution=[hash[i_wholesale_cost]])
                                                   +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0