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
    return len(values)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_8 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_7 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_6 = autonode_8.order_by(col('cr_returning_hdemo_sk_node_8'))
autonode_5 = autonode_7.filter(preloaded_udf_boolean(col('wp_rec_end_date_node_7')))
autonode_4 = autonode_5.join(autonode_6, col('wp_char_count_node_7') == col('cr_call_center_sk_node_8'))
autonode_3 = autonode_4.group_by(col('wp_type_node_7')).select(col('wp_max_ad_count_node_7').min.alias('wp_max_ad_count_node_7'))
autonode_2 = autonode_3.alias('jyxM1')
autonode_1 = autonode_2.alias('4r3OJ')
sink = autonode_1.limit(45)
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
      "error_message": "An error occurred while calling o187842125.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#379798798:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[9](input=RelSubset#379798796,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[9]), rel#379798795:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[9](input=RelSubset#379798794,groupBy=cr_call_center_sk,select=cr_call_center_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (9) must be less than size (1)
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
LogicalSort(fetch=[45])
+- LogicalProject(4r3OJ=[AS(AS($1, _UTF-16LE'jyxM1'), _UTF-16LE'4r3OJ')])
   +- LogicalAggregate(group=[{9}], EXPR$0=[MIN($13)])
      +- LogicalJoin(condition=[=($10, $25)], joinType=[inner])
         :- LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($3)])
         :  +- LogicalProject(wp_web_page_sk_node_7=[$0], wp_web_page_id_node_7=[$1], wp_rec_start_date_node_7=[$2], wp_rec_end_date_node_7=[$3], wp_creation_date_sk_node_7=[$4], wp_access_date_sk_node_7=[$5], wp_autogen_flag_node_7=[$6], wp_customer_sk_node_7=[$7], wp_url_node_7=[$8], wp_type_node_7=[$9], wp_char_count_node_7=[$10], wp_link_count_node_7=[$11], wp_image_count_node_7=[$12], wp_max_ad_count_node_7=[$13])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
         +- LogicalSort(sort0=[$9], dir0=[ASC])
            +- LogicalProject(cr_returned_date_sk_node_8=[$0], cr_returned_time_sk_node_8=[$1], cr_item_sk_node_8=[$2], cr_refunded_customer_sk_node_8=[$3], cr_refunded_cdemo_sk_node_8=[$4], cr_refunded_hdemo_sk_node_8=[$5], cr_refunded_addr_sk_node_8=[$6], cr_returning_customer_sk_node_8=[$7], cr_returning_cdemo_sk_node_8=[$8], cr_returning_hdemo_sk_node_8=[$9], cr_returning_addr_sk_node_8=[$10], cr_call_center_sk_node_8=[$11], cr_catalog_page_sk_node_8=[$12], cr_ship_mode_sk_node_8=[$13], cr_warehouse_sk_node_8=[$14], cr_reason_sk_node_8=[$15], cr_order_number_node_8=[$16], cr_return_quantity_node_8=[$17], cr_return_amount_node_8=[$18], cr_return_tax_node_8=[$19], cr_return_amt_inc_tax_node_8=[$20], cr_fee_node_8=[$21], cr_return_ship_cost_node_8=[$22], cr_refunded_cash_node_8=[$23], cr_reversed_charge_node_8=[$24], cr_store_credit_node_8=[$25], cr_net_loss_node_8=[$26])
               +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[45], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[45], global=[false])
      +- Calc(select=[EXPR$0 AS 4r3OJ])
         +- SortAggregate(isMerge=[true], groupBy=[wp_type], select=[wp_type, Final_MIN(min$0) AS EXPR$0])
            +- Sort(orderBy=[wp_type ASC])
               +- Exchange(distribution=[hash[wp_type]])
                  +- LocalSortAggregate(groupBy=[wp_type], select=[wp_type, Partial_MIN(wp_max_ad_count) AS min$0])
                     +- Sort(orderBy=[wp_type ASC])
                        +- NestedLoopJoin(joinType=[InnerJoin], where=[=(wp_char_count, cr_call_center_sk)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], build=[right])
                           :- Calc(select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], where=[f0])
                           :  +- PythonCalc(select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(wp_rec_end_date) AS f0])
                           :     +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                           +- Exchange(distribution=[broadcast])
                              +- SortLimit(orderBy=[cr_returning_hdemo_sk ASC], offset=[0], fetch=[1], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- SortLimit(orderBy=[cr_returning_hdemo_sk ASC], offset=[0], fetch=[1], global=[false])
                                       +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[45], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[45], global=[false])
      +- Calc(select=[EXPR$0 AS 4r3OJ])
         +- SortAggregate(isMerge=[true], groupBy=[wp_type], select=[wp_type, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[forward])
               +- Sort(orderBy=[wp_type ASC])
                  +- Exchange(distribution=[hash[wp_type]])
                     +- LocalSortAggregate(groupBy=[wp_type], select=[wp_type, Partial_MIN(wp_max_ad_count) AS min$0])
                        +- Exchange(distribution=[forward])
                           +- Sort(orderBy=[wp_type ASC])
                              +- NestedLoopJoin(joinType=[InnerJoin], where=[(wp_char_count = cr_call_center_sk)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], build=[right])
                                 :- Calc(select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], where=[f0])
                                 :  +- PythonCalc(select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(wp_rec_end_date) AS f0])
                                 :     +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                                 +- Exchange(distribution=[broadcast])
                                    +- SortLimit(orderBy=[cr_returning_hdemo_sk ASC], offset=[0], fetch=[1], global=[true])
                                       +- Exchange(distribution=[single])
                                          +- SortLimit(orderBy=[cr_returning_hdemo_sk ASC], offset=[0], fetch=[1], global=[false])
                                             +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0