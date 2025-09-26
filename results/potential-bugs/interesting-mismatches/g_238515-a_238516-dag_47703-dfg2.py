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

autonode_10 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_12 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_11 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_9 = autonode_12.alias('qvxaS')
autonode_8 = autonode_10.join(autonode_11, col('sr_cdemo_sk_node_11') == col('cr_returned_date_sk_node_10'))
autonode_7 = autonode_9.order_by(col('cr_return_amount_node_12'))
autonode_6 = autonode_8.add_columns(lit("hello"))
autonode_5 = autonode_7.order_by(col('cr_net_loss_node_12'))
autonode_4 = autonode_6.limit(50)
autonode_3 = autonode_4.join(autonode_5, col('cr_reversed_charge_node_12') == col('cr_return_amount_node_10'))
autonode_2 = autonode_3.alias('ZItFp')
autonode_1 = autonode_2.filter(col('cr_warehouse_sk_node_10') > -49)
sink = autonode_1.group_by(col('cr_return_ship_cost_node_10')).select(col('cr_warehouse_sk_node_10').max.alias('cr_warehouse_sk_node_10'))
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
      "error_message": "An error occurred while calling o129796073.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#262243588:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[26](input=RelSubset#262243586,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[26]), rel#262243585:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[26](input=RelSubset#262243584,groupBy=cr_reversed_charge,select=cr_reversed_charge)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (26) must be less than size (1)
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
LogicalProject(cr_warehouse_sk_node_10=[$1])
+- LogicalAggregate(group=[{22}], EXPR$0=[MAX($14)])
   +- LogicalFilter(condition=[>($14, -49)])
      +- LogicalProject(ZItFp=[AS($0, _UTF-16LE'ZItFp')], cr_returned_time_sk_node_10=[$1], cr_item_sk_node_10=[$2], cr_refunded_customer_sk_node_10=[$3], cr_refunded_cdemo_sk_node_10=[$4], cr_refunded_hdemo_sk_node_10=[$5], cr_refunded_addr_sk_node_10=[$6], cr_returning_customer_sk_node_10=[$7], cr_returning_cdemo_sk_node_10=[$8], cr_returning_hdemo_sk_node_10=[$9], cr_returning_addr_sk_node_10=[$10], cr_call_center_sk_node_10=[$11], cr_catalog_page_sk_node_10=[$12], cr_ship_mode_sk_node_10=[$13], cr_warehouse_sk_node_10=[$14], cr_reason_sk_node_10=[$15], cr_order_number_node_10=[$16], cr_return_quantity_node_10=[$17], cr_return_amount_node_10=[$18], cr_return_tax_node_10=[$19], cr_return_amt_inc_tax_node_10=[$20], cr_fee_node_10=[$21], cr_return_ship_cost_node_10=[$22], cr_refunded_cash_node_10=[$23], cr_reversed_charge_node_10=[$24], cr_store_credit_node_10=[$25], cr_net_loss_node_10=[$26], sr_returned_date_sk_node_11=[$27], sr_return_time_sk_node_11=[$28], sr_item_sk_node_11=[$29], sr_customer_sk_node_11=[$30], sr_cdemo_sk_node_11=[$31], sr_hdemo_sk_node_11=[$32], sr_addr_sk_node_11=[$33], sr_store_sk_node_11=[$34], sr_reason_sk_node_11=[$35], sr_ticket_number_node_11=[$36], sr_return_quantity_node_11=[$37], sr_return_amt_node_11=[$38], sr_return_tax_node_11=[$39], sr_return_amt_inc_tax_node_11=[$40], sr_fee_node_11=[$41], sr_return_ship_cost_node_11=[$42], sr_refunded_cash_node_11=[$43], sr_reversed_charge_node_11=[$44], sr_store_credit_node_11=[$45], sr_net_loss_node_11=[$46], _c47=[$47], qvxaS=[$48], cr_returned_time_sk_node_12=[$49], cr_item_sk_node_12=[$50], cr_refunded_customer_sk_node_12=[$51], cr_refunded_cdemo_sk_node_12=[$52], cr_refunded_hdemo_sk_node_12=[$53], cr_refunded_addr_sk_node_12=[$54], cr_returning_customer_sk_node_12=[$55], cr_returning_cdemo_sk_node_12=[$56], cr_returning_hdemo_sk_node_12=[$57], cr_returning_addr_sk_node_12=[$58], cr_call_center_sk_node_12=[$59], cr_catalog_page_sk_node_12=[$60], cr_ship_mode_sk_node_12=[$61], cr_warehouse_sk_node_12=[$62], cr_reason_sk_node_12=[$63], cr_order_number_node_12=[$64], cr_return_quantity_node_12=[$65], cr_return_amount_node_12=[$66], cr_return_tax_node_12=[$67], cr_return_amt_inc_tax_node_12=[$68], cr_fee_node_12=[$69], cr_return_ship_cost_node_12=[$70], cr_refunded_cash_node_12=[$71], cr_reversed_charge_node_12=[$72], cr_store_credit_node_12=[$73], cr_net_loss_node_12=[$74])
         +- LogicalJoin(condition=[=($72, $18)], joinType=[inner])
            :- LogicalSort(fetch=[50])
            :  +- LogicalProject(cr_returned_date_sk_node_10=[$0], cr_returned_time_sk_node_10=[$1], cr_item_sk_node_10=[$2], cr_refunded_customer_sk_node_10=[$3], cr_refunded_cdemo_sk_node_10=[$4], cr_refunded_hdemo_sk_node_10=[$5], cr_refunded_addr_sk_node_10=[$6], cr_returning_customer_sk_node_10=[$7], cr_returning_cdemo_sk_node_10=[$8], cr_returning_hdemo_sk_node_10=[$9], cr_returning_addr_sk_node_10=[$10], cr_call_center_sk_node_10=[$11], cr_catalog_page_sk_node_10=[$12], cr_ship_mode_sk_node_10=[$13], cr_warehouse_sk_node_10=[$14], cr_reason_sk_node_10=[$15], cr_order_number_node_10=[$16], cr_return_quantity_node_10=[$17], cr_return_amount_node_10=[$18], cr_return_tax_node_10=[$19], cr_return_amt_inc_tax_node_10=[$20], cr_fee_node_10=[$21], cr_return_ship_cost_node_10=[$22], cr_refunded_cash_node_10=[$23], cr_reversed_charge_node_10=[$24], cr_store_credit_node_10=[$25], cr_net_loss_node_10=[$26], sr_returned_date_sk_node_11=[$27], sr_return_time_sk_node_11=[$28], sr_item_sk_node_11=[$29], sr_customer_sk_node_11=[$30], sr_cdemo_sk_node_11=[$31], sr_hdemo_sk_node_11=[$32], sr_addr_sk_node_11=[$33], sr_store_sk_node_11=[$34], sr_reason_sk_node_11=[$35], sr_ticket_number_node_11=[$36], sr_return_quantity_node_11=[$37], sr_return_amt_node_11=[$38], sr_return_tax_node_11=[$39], sr_return_amt_inc_tax_node_11=[$40], sr_fee_node_11=[$41], sr_return_ship_cost_node_11=[$42], sr_refunded_cash_node_11=[$43], sr_reversed_charge_node_11=[$44], sr_store_credit_node_11=[$45], sr_net_loss_node_11=[$46], _c47=[_UTF-16LE'hello'])
            :     +- LogicalJoin(condition=[=($31, $0)], joinType=[inner])
            :        :- LogicalProject(cr_returned_date_sk_node_10=[$0], cr_returned_time_sk_node_10=[$1], cr_item_sk_node_10=[$2], cr_refunded_customer_sk_node_10=[$3], cr_refunded_cdemo_sk_node_10=[$4], cr_refunded_hdemo_sk_node_10=[$5], cr_refunded_addr_sk_node_10=[$6], cr_returning_customer_sk_node_10=[$7], cr_returning_cdemo_sk_node_10=[$8], cr_returning_hdemo_sk_node_10=[$9], cr_returning_addr_sk_node_10=[$10], cr_call_center_sk_node_10=[$11], cr_catalog_page_sk_node_10=[$12], cr_ship_mode_sk_node_10=[$13], cr_warehouse_sk_node_10=[$14], cr_reason_sk_node_10=[$15], cr_order_number_node_10=[$16], cr_return_quantity_node_10=[$17], cr_return_amount_node_10=[$18], cr_return_tax_node_10=[$19], cr_return_amt_inc_tax_node_10=[$20], cr_fee_node_10=[$21], cr_return_ship_cost_node_10=[$22], cr_refunded_cash_node_10=[$23], cr_reversed_charge_node_10=[$24], cr_store_credit_node_10=[$25], cr_net_loss_node_10=[$26])
            :        :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
            :        +- LogicalProject(sr_returned_date_sk_node_11=[$0], sr_return_time_sk_node_11=[$1], sr_item_sk_node_11=[$2], sr_customer_sk_node_11=[$3], sr_cdemo_sk_node_11=[$4], sr_hdemo_sk_node_11=[$5], sr_addr_sk_node_11=[$6], sr_store_sk_node_11=[$7], sr_reason_sk_node_11=[$8], sr_ticket_number_node_11=[$9], sr_return_quantity_node_11=[$10], sr_return_amt_node_11=[$11], sr_return_tax_node_11=[$12], sr_return_amt_inc_tax_node_11=[$13], sr_fee_node_11=[$14], sr_return_ship_cost_node_11=[$15], sr_refunded_cash_node_11=[$16], sr_reversed_charge_node_11=[$17], sr_store_credit_node_11=[$18], sr_net_loss_node_11=[$19])
            :           +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
            +- LogicalSort(sort0=[$26], dir0=[ASC])
               +- LogicalSort(sort0=[$18], dir0=[ASC])
                  +- LogicalProject(qvxaS=[AS($0, _UTF-16LE'qvxaS')], cr_returned_time_sk_node_12=[$1], cr_item_sk_node_12=[$2], cr_refunded_customer_sk_node_12=[$3], cr_refunded_cdemo_sk_node_12=[$4], cr_refunded_hdemo_sk_node_12=[$5], cr_refunded_addr_sk_node_12=[$6], cr_returning_customer_sk_node_12=[$7], cr_returning_cdemo_sk_node_12=[$8], cr_returning_hdemo_sk_node_12=[$9], cr_returning_addr_sk_node_12=[$10], cr_call_center_sk_node_12=[$11], cr_catalog_page_sk_node_12=[$12], cr_ship_mode_sk_node_12=[$13], cr_warehouse_sk_node_12=[$14], cr_reason_sk_node_12=[$15], cr_order_number_node_12=[$16], cr_return_quantity_node_12=[$17], cr_return_amount_node_12=[$18], cr_return_tax_node_12=[$19], cr_return_amt_inc_tax_node_12=[$20], cr_fee_node_12=[$21], cr_return_ship_cost_node_12=[$22], cr_refunded_cash_node_12=[$23], cr_reversed_charge_node_12=[$24], cr_store_credit_node_12=[$25], cr_net_loss_node_12=[$26])
                     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cr_warehouse_sk_node_10])
+- SortAggregate(isMerge=[true], groupBy=[cr_return_ship_cost_node_10], select=[cr_return_ship_cost_node_10, Final_MAX(max$0) AS EXPR$0])
   +- Sort(orderBy=[cr_return_ship_cost_node_10 ASC])
      +- Exchange(distribution=[hash[cr_return_ship_cost_node_10]])
         +- LocalSortAggregate(groupBy=[cr_return_ship_cost_node_10], select=[cr_return_ship_cost_node_10, Partial_MAX(cr_warehouse_sk_node_10) AS max$0])
            +- Sort(orderBy=[cr_return_ship_cost_node_10 ASC])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cr_reversed_charge_node_12, cr_return_amount_node_10)], select=[cr_returned_date_sk_node_10, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10, sr_returned_date_sk_node_11, sr_return_time_sk_node_11, sr_item_sk_node_11, sr_customer_sk_node_11, sr_cdemo_sk_node_11, sr_hdemo_sk_node_11, sr_addr_sk_node_11, sr_store_sk_node_11, sr_reason_sk_node_11, sr_ticket_number_node_11, sr_return_quantity_node_11, sr_return_amt_node_11, sr_return_tax_node_11, sr_return_amt_inc_tax_node_11, sr_fee_node_11, sr_return_ship_cost_node_11, sr_refunded_cash_node_11, sr_reversed_charge_node_11, sr_store_credit_node_11, sr_net_loss_node_11, qvxaS, cr_returned_time_sk_node_12, cr_item_sk_node_12, cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk_node_12, cr_returning_customer_sk_node_12, cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk_node_12, cr_returning_addr_sk_node_12, cr_call_center_sk_node_12, cr_catalog_page_sk_node_12, cr_ship_mode_sk_node_12, cr_warehouse_sk_node_12, cr_reason_sk_node_12, cr_order_number_node_12, cr_return_quantity_node_12, cr_return_amount_node_12, cr_return_tax_node_12, cr_return_amt_inc_tax_node_12, cr_fee_node_12, cr_return_ship_cost_node_12, cr_refunded_cash_node_12, cr_reversed_charge_node_12, cr_store_credit_node_12, cr_net_loss_node_12], build=[right])
                  :- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_10, cr_returned_time_sk AS cr_returned_time_sk_node_10, cr_item_sk AS cr_item_sk_node_10, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_10, cr_returning_customer_sk AS cr_returning_customer_sk_node_10, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_10, cr_returning_addr_sk AS cr_returning_addr_sk_node_10, cr_call_center_sk AS cr_call_center_sk_node_10, cr_catalog_page_sk AS cr_catalog_page_sk_node_10, cr_ship_mode_sk AS cr_ship_mode_sk_node_10, cr_warehouse_sk AS cr_warehouse_sk_node_10, cr_reason_sk AS cr_reason_sk_node_10, cr_order_number AS cr_order_number_node_10, cr_return_quantity AS cr_return_quantity_node_10, cr_return_amount AS cr_return_amount_node_10, cr_return_tax AS cr_return_tax_node_10, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_10, cr_fee AS cr_fee_node_10, cr_return_ship_cost AS cr_return_ship_cost_node_10, cr_refunded_cash AS cr_refunded_cash_node_10, cr_reversed_charge AS cr_reversed_charge_node_10, cr_store_credit AS cr_store_credit_node_10, cr_net_loss AS cr_net_loss_node_10, sr_returned_date_sk AS sr_returned_date_sk_node_11, sr_return_time_sk AS sr_return_time_sk_node_11, sr_item_sk AS sr_item_sk_node_11, sr_customer_sk AS sr_customer_sk_node_11, sr_cdemo_sk AS sr_cdemo_sk_node_11, sr_hdemo_sk AS sr_hdemo_sk_node_11, sr_addr_sk AS sr_addr_sk_node_11, sr_store_sk AS sr_store_sk_node_11, sr_reason_sk AS sr_reason_sk_node_11, sr_ticket_number AS sr_ticket_number_node_11, sr_return_quantity AS sr_return_quantity_node_11, sr_return_amt AS sr_return_amt_node_11, sr_return_tax AS sr_return_tax_node_11, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_11, sr_fee AS sr_fee_node_11, sr_return_ship_cost AS sr_return_ship_cost_node_11, sr_refunded_cash AS sr_refunded_cash_node_11, sr_reversed_charge AS sr_reversed_charge_node_11, sr_store_credit AS sr_store_credit_node_11, sr_net_loss AS sr_net_loss_node_11], where=[>(cr_warehouse_sk, -49)])
                  :  +- Limit(offset=[0], fetch=[50], global=[true])
                  :     +- Exchange(distribution=[single])
                  :        +- Limit(offset=[0], fetch=[50], global=[false])
                  :           +- HashJoin(joinType=[InnerJoin], where=[=(sr_cdemo_sk, cr_returned_date_sk)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], build=[left])
                  :              :- Exchange(distribution=[hash[cr_returned_date_sk]])
                  :              :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                  :              +- Exchange(distribution=[hash[sr_cdemo_sk]])
                  :                 +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                  +- Exchange(distribution=[broadcast])
                     +- SortLimit(orderBy=[cr_net_loss_node_12 ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[cr_net_loss_node_12 ASC], offset=[0], fetch=[1], global=[false])
                              +- Calc(select=[cr_returned_date_sk AS qvxaS, cr_returned_time_sk AS cr_returned_time_sk_node_12, cr_item_sk AS cr_item_sk_node_12, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_12, cr_returning_customer_sk AS cr_returning_customer_sk_node_12, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_12, cr_returning_addr_sk AS cr_returning_addr_sk_node_12, cr_call_center_sk AS cr_call_center_sk_node_12, cr_catalog_page_sk AS cr_catalog_page_sk_node_12, cr_ship_mode_sk AS cr_ship_mode_sk_node_12, cr_warehouse_sk AS cr_warehouse_sk_node_12, cr_reason_sk AS cr_reason_sk_node_12, cr_order_number AS cr_order_number_node_12, cr_return_quantity AS cr_return_quantity_node_12, cr_return_amount AS cr_return_amount_node_12, cr_return_tax AS cr_return_tax_node_12, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_12, cr_fee AS cr_fee_node_12, cr_return_ship_cost AS cr_return_ship_cost_node_12, cr_refunded_cash AS cr_refunded_cash_node_12, cr_reversed_charge AS cr_reversed_charge_node_12, cr_store_credit AS cr_store_credit_node_12, cr_net_loss AS cr_net_loss_node_12])
                                 +- SortLimit(orderBy=[cr_return_amount ASC], offset=[0], fetch=[1], global=[true])
                                    +- Exchange(distribution=[single])
                                       +- SortLimit(orderBy=[cr_return_amount ASC], offset=[0], fetch=[1], global=[false])
                                          +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cr_warehouse_sk_node_10])
+- SortAggregate(isMerge=[true], groupBy=[cr_return_ship_cost_node_10], select=[cr_return_ship_cost_node_10, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[cr_return_ship_cost_node_10 ASC])
         +- Exchange(distribution=[hash[cr_return_ship_cost_node_10]])
            +- LocalSortAggregate(groupBy=[cr_return_ship_cost_node_10], select=[cr_return_ship_cost_node_10, Partial_MAX(cr_warehouse_sk_node_10) AS max$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[cr_return_ship_cost_node_10 ASC])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(cr_reversed_charge_node_12 = cr_return_amount_node_10)], select=[cr_returned_date_sk_node_10, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10, sr_returned_date_sk_node_11, sr_return_time_sk_node_11, sr_item_sk_node_11, sr_customer_sk_node_11, sr_cdemo_sk_node_11, sr_hdemo_sk_node_11, sr_addr_sk_node_11, sr_store_sk_node_11, sr_reason_sk_node_11, sr_ticket_number_node_11, sr_return_quantity_node_11, sr_return_amt_node_11, sr_return_tax_node_11, sr_return_amt_inc_tax_node_11, sr_fee_node_11, sr_return_ship_cost_node_11, sr_refunded_cash_node_11, sr_reversed_charge_node_11, sr_store_credit_node_11, sr_net_loss_node_11, qvxaS, cr_returned_time_sk_node_12, cr_item_sk_node_12, cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk_node_12, cr_returning_customer_sk_node_12, cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk_node_12, cr_returning_addr_sk_node_12, cr_call_center_sk_node_12, cr_catalog_page_sk_node_12, cr_ship_mode_sk_node_12, cr_warehouse_sk_node_12, cr_reason_sk_node_12, cr_order_number_node_12, cr_return_quantity_node_12, cr_return_amount_node_12, cr_return_tax_node_12, cr_return_amt_inc_tax_node_12, cr_fee_node_12, cr_return_ship_cost_node_12, cr_refunded_cash_node_12, cr_reversed_charge_node_12, cr_store_credit_node_12, cr_net_loss_node_12], build=[right])
                        :- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_10, cr_returned_time_sk AS cr_returned_time_sk_node_10, cr_item_sk AS cr_item_sk_node_10, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_10, cr_returning_customer_sk AS cr_returning_customer_sk_node_10, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_10, cr_returning_addr_sk AS cr_returning_addr_sk_node_10, cr_call_center_sk AS cr_call_center_sk_node_10, cr_catalog_page_sk AS cr_catalog_page_sk_node_10, cr_ship_mode_sk AS cr_ship_mode_sk_node_10, cr_warehouse_sk AS cr_warehouse_sk_node_10, cr_reason_sk AS cr_reason_sk_node_10, cr_order_number AS cr_order_number_node_10, cr_return_quantity AS cr_return_quantity_node_10, cr_return_amount AS cr_return_amount_node_10, cr_return_tax AS cr_return_tax_node_10, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_10, cr_fee AS cr_fee_node_10, cr_return_ship_cost AS cr_return_ship_cost_node_10, cr_refunded_cash AS cr_refunded_cash_node_10, cr_reversed_charge AS cr_reversed_charge_node_10, cr_store_credit AS cr_store_credit_node_10, cr_net_loss AS cr_net_loss_node_10, sr_returned_date_sk AS sr_returned_date_sk_node_11, sr_return_time_sk AS sr_return_time_sk_node_11, sr_item_sk AS sr_item_sk_node_11, sr_customer_sk AS sr_customer_sk_node_11, sr_cdemo_sk AS sr_cdemo_sk_node_11, sr_hdemo_sk AS sr_hdemo_sk_node_11, sr_addr_sk AS sr_addr_sk_node_11, sr_store_sk AS sr_store_sk_node_11, sr_reason_sk AS sr_reason_sk_node_11, sr_ticket_number AS sr_ticket_number_node_11, sr_return_quantity AS sr_return_quantity_node_11, sr_return_amt AS sr_return_amt_node_11, sr_return_tax AS sr_return_tax_node_11, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_11, sr_fee AS sr_fee_node_11, sr_return_ship_cost AS sr_return_ship_cost_node_11, sr_refunded_cash AS sr_refunded_cash_node_11, sr_reversed_charge AS sr_reversed_charge_node_11, sr_store_credit AS sr_store_credit_node_11, sr_net_loss AS sr_net_loss_node_11], where=[(cr_warehouse_sk > -49)])
                        :  +- Limit(offset=[0], fetch=[50], global=[true])
                        :     +- Exchange(distribution=[single])
                        :        +- Limit(offset=[0], fetch=[50], global=[false])
                        :           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(sr_cdemo_sk = cr_returned_date_sk)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], build=[left])
                        :              :- Exchange(distribution=[hash[cr_returned_date_sk]])
                        :              :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                        :              +- Exchange(distribution=[hash[sr_cdemo_sk]])
                        :                 +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                        +- Exchange(distribution=[broadcast])
                           +- SortLimit(orderBy=[cr_net_loss_node_12 ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[cr_net_loss_node_12 ASC], offset=[0], fetch=[1], global=[false])
                                    +- Calc(select=[cr_returned_date_sk AS qvxaS, cr_returned_time_sk AS cr_returned_time_sk_node_12, cr_item_sk AS cr_item_sk_node_12, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_12, cr_returning_customer_sk AS cr_returning_customer_sk_node_12, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_12, cr_returning_addr_sk AS cr_returning_addr_sk_node_12, cr_call_center_sk AS cr_call_center_sk_node_12, cr_catalog_page_sk AS cr_catalog_page_sk_node_12, cr_ship_mode_sk AS cr_ship_mode_sk_node_12, cr_warehouse_sk AS cr_warehouse_sk_node_12, cr_reason_sk AS cr_reason_sk_node_12, cr_order_number AS cr_order_number_node_12, cr_return_quantity AS cr_return_quantity_node_12, cr_return_amount AS cr_return_amount_node_12, cr_return_tax AS cr_return_tax_node_12, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_12, cr_fee AS cr_fee_node_12, cr_return_ship_cost AS cr_return_ship_cost_node_12, cr_refunded_cash AS cr_refunded_cash_node_12, cr_reversed_charge AS cr_reversed_charge_node_12, cr_store_credit AS cr_store_credit_node_12, cr_net_loss AS cr_net_loss_node_12])
                                       +- SortLimit(orderBy=[cr_return_amount ASC], offset=[0], fetch=[1], global=[true])
                                          +- Exchange(distribution=[single])
                                             +- SortLimit(orderBy=[cr_return_amount ASC], offset=[0], fetch=[1], global=[false])
                                                +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0