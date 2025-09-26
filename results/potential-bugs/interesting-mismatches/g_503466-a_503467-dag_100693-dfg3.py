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

autonode_9 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_8 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_7 = autonode_9.limit(74)
autonode_6 = autonode_8.distinct()
autonode_5 = autonode_7.alias('sS1EW')
autonode_4 = autonode_6.order_by(col('sr_refunded_cash_node_8'))
autonode_3 = autonode_4.join(autonode_5, col('w_gmt_offset_node_9') == col('sr_fee_node_8'))
autonode_2 = autonode_3.group_by(col('w_warehouse_sq_ft_node_9')).select(col('w_street_type_node_9').min.alias('w_street_type_node_9'))
autonode_1 = autonode_2.filter(col('w_street_type_node_9').char_length <= 5)
sink = autonode_1.limit(43)
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
      "error_message": "An error occurred while calling o274140857.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#554545619:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[16](input=RelSubset#554545617,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[16]), rel#554545616:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[16](input=RelSubset#554545615,groupBy=sr_fee,select=sr_fee)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (16) must be less than size (1)
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
LogicalSort(fetch=[43])
+- LogicalFilter(condition=[<=(CHAR_LENGTH($0), 5)])
   +- LogicalProject(w_street_type_node_9=[$1])
      +- LogicalAggregate(group=[{23}], EXPR$0=[MIN($26)])
         +- LogicalJoin(condition=[=($33, $14)], joinType=[inner])
            :- LogicalSort(sort0=[$16], dir0=[ASC])
            :  +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}])
            :     +- LogicalProject(sr_returned_date_sk_node_8=[$0], sr_return_time_sk_node_8=[$1], sr_item_sk_node_8=[$2], sr_customer_sk_node_8=[$3], sr_cdemo_sk_node_8=[$4], sr_hdemo_sk_node_8=[$5], sr_addr_sk_node_8=[$6], sr_store_sk_node_8=[$7], sr_reason_sk_node_8=[$8], sr_ticket_number_node_8=[$9], sr_return_quantity_node_8=[$10], sr_return_amt_node_8=[$11], sr_return_tax_node_8=[$12], sr_return_amt_inc_tax_node_8=[$13], sr_fee_node_8=[$14], sr_return_ship_cost_node_8=[$15], sr_refunded_cash_node_8=[$16], sr_reversed_charge_node_8=[$17], sr_store_credit_node_8=[$18], sr_net_loss_node_8=[$19])
            :        +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
            +- LogicalProject(sS1EW=[AS($0, _UTF-16LE'sS1EW')], w_warehouse_id_node_9=[$1], w_warehouse_name_node_9=[$2], w_warehouse_sq_ft_node_9=[$3], w_street_number_node_9=[$4], w_street_name_node_9=[$5], w_street_type_node_9=[$6], w_suite_number_node_9=[$7], w_city_node_9=[$8], w_county_node_9=[$9], w_state_node_9=[$10], w_zip_node_9=[$11], w_country_node_9=[$12], w_gmt_offset_node_9=[$13])
               +- LogicalSort(fetch=[74])
                  +- LogicalProject(w_warehouse_sk_node_9=[$0], w_warehouse_id_node_9=[$1], w_warehouse_name_node_9=[$2], w_warehouse_sq_ft_node_9=[$3], w_street_number_node_9=[$4], w_street_name_node_9=[$5], w_street_type_node_9=[$6], w_suite_number_node_9=[$7], w_city_node_9=[$8], w_county_node_9=[$9], w_state_node_9=[$10], w_zip_node_9=[$11], w_country_node_9=[$12], w_gmt_offset_node_9=[$13])
                     +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[43], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[43], global=[false])
      +- Calc(select=[EXPR$0], where=[<=(CHAR_LENGTH(EXPR$0), 5)])
         +- SortAggregate(isMerge=[false], groupBy=[w_warehouse_sq_ft_node_9], select=[w_warehouse_sq_ft_node_9, MIN(w_street_type_node_9) AS EXPR$0])
            +- Sort(orderBy=[w_warehouse_sq_ft_node_9 ASC])
               +- Exchange(distribution=[hash[w_warehouse_sq_ft_node_9]])
                  +- Calc(select=[sr_returned_date_sk AS sr_returned_date_sk_node_8, sr_return_time_sk AS sr_return_time_sk_node_8, sr_item_sk AS sr_item_sk_node_8, sr_customer_sk AS sr_customer_sk_node_8, sr_cdemo_sk AS sr_cdemo_sk_node_8, sr_hdemo_sk AS sr_hdemo_sk_node_8, sr_addr_sk AS sr_addr_sk_node_8, sr_store_sk AS sr_store_sk_node_8, sr_reason_sk AS sr_reason_sk_node_8, sr_ticket_number AS sr_ticket_number_node_8, sr_return_quantity AS sr_return_quantity_node_8, sr_return_amt AS sr_return_amt_node_8, sr_return_tax AS sr_return_tax_node_8, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_8, sr_fee AS sr_fee_node_8, sr_return_ship_cost AS sr_return_ship_cost_node_8, sr_refunded_cash AS sr_refunded_cash_node_8, sr_reversed_charge AS sr_reversed_charge_node_8, sr_store_credit AS sr_store_credit_node_8, sr_net_loss AS sr_net_loss_node_8, sS1EW, w_warehouse_id_node_9, w_warehouse_name_node_9, w_warehouse_sq_ft_node_9, w_street_number_node_9, w_street_name_node_9, w_street_type_node_9, w_suite_number_node_9, w_city_node_9, w_county_node_9, w_state_node_9, w_zip_node_9, w_country_node_9, w_gmt_offset_node_9])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(w_gmt_offset_node_90, sr_fee)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, sS1EW, w_warehouse_id_node_9, w_warehouse_name_node_9, w_warehouse_sq_ft_node_9, w_street_number_node_9, w_street_name_node_9, w_street_type_node_9, w_suite_number_node_9, w_city_node_9, w_county_node_9, w_state_node_9, w_zip_node_9, w_country_node_9, w_gmt_offset_node_9, w_gmt_offset_node_90], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- SortLimit(orderBy=[sr_refunded_cash ASC], offset=[0], fetch=[1], global=[true])
                        :     +- Exchange(distribution=[single])
                        :        +- SortLimit(orderBy=[sr_refunded_cash ASC], offset=[0], fetch=[1], global=[false])
                        :           +- HashAggregate(isMerge=[false], groupBy=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                        :              +- Exchange(distribution=[hash[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss]])
                        :                 +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                        +- Calc(select=[w_warehouse_sk AS sS1EW, w_warehouse_id AS w_warehouse_id_node_9, w_warehouse_name AS w_warehouse_name_node_9, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_9, w_street_number AS w_street_number_node_9, w_street_name AS w_street_name_node_9, w_street_type AS w_street_type_node_9, w_suite_number AS w_suite_number_node_9, w_city AS w_city_node_9, w_county AS w_county_node_9, w_state AS w_state_node_9, w_zip AS w_zip_node_9, w_country AS w_country_node_9, w_gmt_offset AS w_gmt_offset_node_9, CAST(w_gmt_offset AS DECIMAL(7, 2)) AS w_gmt_offset_node_90])
                           +- Limit(offset=[0], fetch=[74], global=[true])
                              +- Exchange(distribution=[single])
                                 +- Limit(offset=[0], fetch=[74], global=[false])
                                    +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[43], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[43], global=[false])
      +- Calc(select=[EXPR$0], where=[(CHAR_LENGTH(EXPR$0) <= 5)])
         +- SortAggregate(isMerge=[false], groupBy=[w_warehouse_sq_ft_node_9], select=[w_warehouse_sq_ft_node_9, MIN(w_street_type_node_9) AS EXPR$0])
            +- Exchange(distribution=[forward])
               +- Sort(orderBy=[w_warehouse_sq_ft_node_9 ASC])
                  +- Exchange(distribution=[hash[w_warehouse_sq_ft_node_9]])
                     +- Calc(select=[sr_returned_date_sk AS sr_returned_date_sk_node_8, sr_return_time_sk AS sr_return_time_sk_node_8, sr_item_sk AS sr_item_sk_node_8, sr_customer_sk AS sr_customer_sk_node_8, sr_cdemo_sk AS sr_cdemo_sk_node_8, sr_hdemo_sk AS sr_hdemo_sk_node_8, sr_addr_sk AS sr_addr_sk_node_8, sr_store_sk AS sr_store_sk_node_8, sr_reason_sk AS sr_reason_sk_node_8, sr_ticket_number AS sr_ticket_number_node_8, sr_return_quantity AS sr_return_quantity_node_8, sr_return_amt AS sr_return_amt_node_8, sr_return_tax AS sr_return_tax_node_8, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_8, sr_fee AS sr_fee_node_8, sr_return_ship_cost AS sr_return_ship_cost_node_8, sr_refunded_cash AS sr_refunded_cash_node_8, sr_reversed_charge AS sr_reversed_charge_node_8, sr_store_credit AS sr_store_credit_node_8, sr_net_loss AS sr_net_loss_node_8, sS1EW, w_warehouse_id_node_9, w_warehouse_name_node_9, w_warehouse_sq_ft_node_9, w_street_number_node_9, w_street_name_node_9, w_street_type_node_9, w_suite_number_node_9, w_city_node_9, w_county_node_9, w_state_node_9, w_zip_node_9, w_country_node_9, w_gmt_offset_node_9])
                        +- NestedLoopJoin(joinType=[InnerJoin], where=[(w_gmt_offset_node_90 = sr_fee)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, sS1EW, w_warehouse_id_node_9, w_warehouse_name_node_9, w_warehouse_sq_ft_node_9, w_street_number_node_9, w_street_name_node_9, w_street_type_node_9, w_suite_number_node_9, w_city_node_9, w_county_node_9, w_state_node_9, w_zip_node_9, w_country_node_9, w_gmt_offset_node_9, w_gmt_offset_node_90], build=[left])
                           :- Exchange(distribution=[broadcast])
                           :  +- SortLimit(orderBy=[sr_refunded_cash ASC], offset=[0], fetch=[1], global=[true])
                           :     +- Exchange(distribution=[single])
                           :        +- SortLimit(orderBy=[sr_refunded_cash ASC], offset=[0], fetch=[1], global=[false])
                           :           +- HashAggregate(isMerge=[false], groupBy=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                           :              +- Exchange(distribution=[hash[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss]])
                           :                 +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                           +- Calc(select=[w_warehouse_sk AS sS1EW, w_warehouse_id AS w_warehouse_id_node_9, w_warehouse_name AS w_warehouse_name_node_9, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_9, w_street_number AS w_street_number_node_9, w_street_name AS w_street_name_node_9, w_street_type AS w_street_type_node_9, w_suite_number AS w_suite_number_node_9, w_city AS w_city_node_9, w_county AS w_county_node_9, w_state AS w_state_node_9, w_zip AS w_zip_node_9, w_country AS w_country_node_9, w_gmt_offset AS w_gmt_offset_node_9, CAST(w_gmt_offset AS DECIMAL(7, 2)) AS w_gmt_offset_node_90])
                              +- Limit(offset=[0], fetch=[74], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- Limit(offset=[0], fetch=[74], global=[false])
                                       +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0