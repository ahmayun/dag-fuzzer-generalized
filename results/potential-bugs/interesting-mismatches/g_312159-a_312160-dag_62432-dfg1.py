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
    return values.quantile(0.75)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_7 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_4 = autonode_6.order_by(col('cr_returning_addr_sk_node_6'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_2 = autonode_4.add_columns(lit("hello"))
autonode_3 = autonode_5.limit(64)
autonode_1 = autonode_2.join(autonode_3, col('cc_division_node_7') == col('cr_warehouse_sk_node_6'))
sink = autonode_1.group_by(col('cc_rec_end_date_node_7')).select(col('cc_hours_node_7').max.alias('cc_hours_node_7'))
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
      "error_message": "An error occurred while calling o169788592.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#343240285:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[10](input=RelSubset#343240283,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[10]), rel#343240282:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[10](input=RelSubset#343240281,groupBy=cr_warehouse_sk,select=cr_warehouse_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (10) must be less than size (1)
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
LogicalProject(cc_hours_node_7=[$1])
+- LogicalAggregate(group=[{31}], EXPR$0=[MAX($38)])
   +- LogicalJoin(condition=[=($44, $14)], joinType=[inner])
      :- LogicalProject(cr_returned_date_sk_node_6=[$0], cr_returned_time_sk_node_6=[$1], cr_item_sk_node_6=[$2], cr_refunded_customer_sk_node_6=[$3], cr_refunded_cdemo_sk_node_6=[$4], cr_refunded_hdemo_sk_node_6=[$5], cr_refunded_addr_sk_node_6=[$6], cr_returning_customer_sk_node_6=[$7], cr_returning_cdemo_sk_node_6=[$8], cr_returning_hdemo_sk_node_6=[$9], cr_returning_addr_sk_node_6=[$10], cr_call_center_sk_node_6=[$11], cr_catalog_page_sk_node_6=[$12], cr_ship_mode_sk_node_6=[$13], cr_warehouse_sk_node_6=[$14], cr_reason_sk_node_6=[$15], cr_order_number_node_6=[$16], cr_return_quantity_node_6=[$17], cr_return_amount_node_6=[$18], cr_return_tax_node_6=[$19], cr_return_amt_inc_tax_node_6=[$20], cr_fee_node_6=[$21], cr_return_ship_cost_node_6=[$22], cr_refunded_cash_node_6=[$23], cr_reversed_charge_node_6=[$24], cr_store_credit_node_6=[$25], cr_net_loss_node_6=[$26], _c27=[_UTF-16LE'hello'])
      :  +- LogicalSort(sort0=[$10], dir0=[ASC])
      :     +- LogicalProject(cr_returned_date_sk_node_6=[$0], cr_returned_time_sk_node_6=[$1], cr_item_sk_node_6=[$2], cr_refunded_customer_sk_node_6=[$3], cr_refunded_cdemo_sk_node_6=[$4], cr_refunded_hdemo_sk_node_6=[$5], cr_refunded_addr_sk_node_6=[$6], cr_returning_customer_sk_node_6=[$7], cr_returning_cdemo_sk_node_6=[$8], cr_returning_hdemo_sk_node_6=[$9], cr_returning_addr_sk_node_6=[$10], cr_call_center_sk_node_6=[$11], cr_catalog_page_sk_node_6=[$12], cr_ship_mode_sk_node_6=[$13], cr_warehouse_sk_node_6=[$14], cr_reason_sk_node_6=[$15], cr_order_number_node_6=[$16], cr_return_quantity_node_6=[$17], cr_return_amount_node_6=[$18], cr_return_tax_node_6=[$19], cr_return_amt_inc_tax_node_6=[$20], cr_fee_node_6=[$21], cr_return_ship_cost_node_6=[$22], cr_refunded_cash_node_6=[$23], cr_reversed_charge_node_6=[$24], cr_store_credit_node_6=[$25], cr_net_loss_node_6=[$26])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
      +- LogicalSort(fetch=[64])
         +- LogicalProject(cc_call_center_sk_node_7=[$0], cc_call_center_id_node_7=[$1], cc_rec_start_date_node_7=[$2], cc_rec_end_date_node_7=[$3], cc_closed_date_sk_node_7=[$4], cc_open_date_sk_node_7=[$5], cc_name_node_7=[$6], cc_class_node_7=[$7], cc_employees_node_7=[$8], cc_sq_ft_node_7=[$9], cc_hours_node_7=[$10], cc_manager_node_7=[$11], cc_mkt_id_node_7=[$12], cc_mkt_class_node_7=[$13], cc_mkt_desc_node_7=[$14], cc_market_manager_node_7=[$15], cc_division_node_7=[$16], cc_division_name_node_7=[$17], cc_company_node_7=[$18], cc_company_name_node_7=[$19], cc_street_number_node_7=[$20], cc_street_name_node_7=[$21], cc_street_type_node_7=[$22], cc_suite_number_node_7=[$23], cc_city_node_7=[$24], cc_county_node_7=[$25], cc_state_node_7=[$26], cc_zip_node_7=[$27], cc_country_node_7=[$28], cc_gmt_offset_node_7=[$29], cc_tax_percentage_node_7=[$30], _c31=[_UTF-16LE'hello'])
            +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cc_hours_node_7])
+- SortAggregate(isMerge=[false], groupBy=[cc_rec_end_date_node_7], select=[cc_rec_end_date_node_7, MAX(cc_hours_node_7) AS EXPR$0])
   +- Sort(orderBy=[cc_rec_end_date_node_7 ASC])
      +- Exchange(distribution=[hash[cc_rec_end_date_node_7]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cc_division_node_7, cr_warehouse_sk_node_6)], select=[cr_returned_date_sk_node_6, cr_returned_time_sk_node_6, cr_item_sk_node_6, cr_refunded_customer_sk_node_6, cr_refunded_cdemo_sk_node_6, cr_refunded_hdemo_sk_node_6, cr_refunded_addr_sk_node_6, cr_returning_customer_sk_node_6, cr_returning_cdemo_sk_node_6, cr_returning_hdemo_sk_node_6, cr_returning_addr_sk_node_6, cr_call_center_sk_node_6, cr_catalog_page_sk_node_6, cr_ship_mode_sk_node_6, cr_warehouse_sk_node_6, cr_reason_sk_node_6, cr_order_number_node_6, cr_return_quantity_node_6, cr_return_amount_node_6, cr_return_tax_node_6, cr_return_amt_inc_tax_node_6, cr_fee_node_6, cr_return_ship_cost_node_6, cr_refunded_cash_node_6, cr_reversed_charge_node_6, cr_store_credit_node_6, cr_net_loss_node_6, _c27, cc_call_center_sk_node_7, cc_call_center_id_node_7, cc_rec_start_date_node_7, cc_rec_end_date_node_7, cc_closed_date_sk_node_7, cc_open_date_sk_node_7, cc_name_node_7, cc_class_node_7, cc_employees_node_7, cc_sq_ft_node_7, cc_hours_node_7, cc_manager_node_7, cc_mkt_id_node_7, cc_mkt_class_node_7, cc_mkt_desc_node_7, cc_market_manager_node_7, cc_division_node_7, cc_division_name_node_7, cc_company_node_7, cc_company_name_node_7, cc_street_number_node_7, cc_street_name_node_7, cc_street_type_node_7, cc_suite_number_node_7, cc_city_node_7, cc_county_node_7, cc_state_node_7, cc_zip_node_7, cc_country_node_7, cc_gmt_offset_node_7, cc_tax_percentage_node_7, _c31], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_6, cr_returned_time_sk AS cr_returned_time_sk_node_6, cr_item_sk AS cr_item_sk_node_6, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_6, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_6, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_6, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_6, cr_returning_customer_sk AS cr_returning_customer_sk_node_6, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_6, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_6, cr_returning_addr_sk AS cr_returning_addr_sk_node_6, cr_call_center_sk AS cr_call_center_sk_node_6, cr_catalog_page_sk AS cr_catalog_page_sk_node_6, cr_ship_mode_sk AS cr_ship_mode_sk_node_6, cr_warehouse_sk AS cr_warehouse_sk_node_6, cr_reason_sk AS cr_reason_sk_node_6, cr_order_number AS cr_order_number_node_6, cr_return_quantity AS cr_return_quantity_node_6, cr_return_amount AS cr_return_amount_node_6, cr_return_tax AS cr_return_tax_node_6, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_6, cr_fee AS cr_fee_node_6, cr_return_ship_cost AS cr_return_ship_cost_node_6, cr_refunded_cash AS cr_refunded_cash_node_6, cr_reversed_charge AS cr_reversed_charge_node_6, cr_store_credit AS cr_store_credit_node_6, cr_net_loss AS cr_net_loss_node_6, 'hello' AS _c27])
            :     +- SortLimit(orderBy=[cr_returning_addr_sk ASC], offset=[0], fetch=[1], global=[true])
            :        +- Exchange(distribution=[single])
            :           +- SortLimit(orderBy=[cr_returning_addr_sk ASC], offset=[0], fetch=[1], global=[false])
            :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
            +- Limit(offset=[0], fetch=[64], global=[true])
               +- Exchange(distribution=[single])
                  +- Limit(offset=[0], fetch=[64], global=[false])
                     +- Calc(select=[cc_call_center_sk AS cc_call_center_sk_node_7, cc_call_center_id AS cc_call_center_id_node_7, cc_rec_start_date AS cc_rec_start_date_node_7, cc_rec_end_date AS cc_rec_end_date_node_7, cc_closed_date_sk AS cc_closed_date_sk_node_7, cc_open_date_sk AS cc_open_date_sk_node_7, cc_name AS cc_name_node_7, cc_class AS cc_class_node_7, cc_employees AS cc_employees_node_7, cc_sq_ft AS cc_sq_ft_node_7, cc_hours AS cc_hours_node_7, cc_manager AS cc_manager_node_7, cc_mkt_id AS cc_mkt_id_node_7, cc_mkt_class AS cc_mkt_class_node_7, cc_mkt_desc AS cc_mkt_desc_node_7, cc_market_manager AS cc_market_manager_node_7, cc_division AS cc_division_node_7, cc_division_name AS cc_division_name_node_7, cc_company AS cc_company_node_7, cc_company_name AS cc_company_name_node_7, cc_street_number AS cc_street_number_node_7, cc_street_name AS cc_street_name_node_7, cc_street_type AS cc_street_type_node_7, cc_suite_number AS cc_suite_number_node_7, cc_city AS cc_city_node_7, cc_county AS cc_county_node_7, cc_state AS cc_state_node_7, cc_zip AS cc_zip_node_7, cc_country AS cc_country_node_7, cc_gmt_offset AS cc_gmt_offset_node_7, cc_tax_percentage AS cc_tax_percentage_node_7, 'hello' AS _c31])
                        +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cc_hours_node_7])
+- SortAggregate(isMerge=[false], groupBy=[cc_rec_end_date_node_7], select=[cc_rec_end_date_node_7, MAX(cc_hours_node_7) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[cc_rec_end_date_node_7 ASC])
         +- Exchange(distribution=[hash[cc_rec_end_date_node_7]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(cc_division_node_7 = cr_warehouse_sk_node_6)], select=[cr_returned_date_sk_node_6, cr_returned_time_sk_node_6, cr_item_sk_node_6, cr_refunded_customer_sk_node_6, cr_refunded_cdemo_sk_node_6, cr_refunded_hdemo_sk_node_6, cr_refunded_addr_sk_node_6, cr_returning_customer_sk_node_6, cr_returning_cdemo_sk_node_6, cr_returning_hdemo_sk_node_6, cr_returning_addr_sk_node_6, cr_call_center_sk_node_6, cr_catalog_page_sk_node_6, cr_ship_mode_sk_node_6, cr_warehouse_sk_node_6, cr_reason_sk_node_6, cr_order_number_node_6, cr_return_quantity_node_6, cr_return_amount_node_6, cr_return_tax_node_6, cr_return_amt_inc_tax_node_6, cr_fee_node_6, cr_return_ship_cost_node_6, cr_refunded_cash_node_6, cr_reversed_charge_node_6, cr_store_credit_node_6, cr_net_loss_node_6, _c27, cc_call_center_sk_node_7, cc_call_center_id_node_7, cc_rec_start_date_node_7, cc_rec_end_date_node_7, cc_closed_date_sk_node_7, cc_open_date_sk_node_7, cc_name_node_7, cc_class_node_7, cc_employees_node_7, cc_sq_ft_node_7, cc_hours_node_7, cc_manager_node_7, cc_mkt_id_node_7, cc_mkt_class_node_7, cc_mkt_desc_node_7, cc_market_manager_node_7, cc_division_node_7, cc_division_name_node_7, cc_company_node_7, cc_company_name_node_7, cc_street_number_node_7, cc_street_name_node_7, cc_street_type_node_7, cc_suite_number_node_7, cc_city_node_7, cc_county_node_7, cc_state_node_7, cc_zip_node_7, cc_country_node_7, cc_gmt_offset_node_7, cc_tax_percentage_node_7, _c31], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_6, cr_returned_time_sk AS cr_returned_time_sk_node_6, cr_item_sk AS cr_item_sk_node_6, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_6, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_6, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_6, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_6, cr_returning_customer_sk AS cr_returning_customer_sk_node_6, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_6, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_6, cr_returning_addr_sk AS cr_returning_addr_sk_node_6, cr_call_center_sk AS cr_call_center_sk_node_6, cr_catalog_page_sk AS cr_catalog_page_sk_node_6, cr_ship_mode_sk AS cr_ship_mode_sk_node_6, cr_warehouse_sk AS cr_warehouse_sk_node_6, cr_reason_sk AS cr_reason_sk_node_6, cr_order_number AS cr_order_number_node_6, cr_return_quantity AS cr_return_quantity_node_6, cr_return_amount AS cr_return_amount_node_6, cr_return_tax AS cr_return_tax_node_6, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_6, cr_fee AS cr_fee_node_6, cr_return_ship_cost AS cr_return_ship_cost_node_6, cr_refunded_cash AS cr_refunded_cash_node_6, cr_reversed_charge AS cr_reversed_charge_node_6, cr_store_credit AS cr_store_credit_node_6, cr_net_loss AS cr_net_loss_node_6, 'hello' AS _c27])
               :     +- SortLimit(orderBy=[cr_returning_addr_sk ASC], offset=[0], fetch=[1], global=[true])
               :        +- Exchange(distribution=[single])
               :           +- SortLimit(orderBy=[cr_returning_addr_sk ASC], offset=[0], fetch=[1], global=[false])
               :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
               +- Limit(offset=[0], fetch=[64], global=[true])
                  +- Exchange(distribution=[single])
                     +- Limit(offset=[0], fetch=[64], global=[false])
                        +- Calc(select=[cc_call_center_sk AS cc_call_center_sk_node_7, cc_call_center_id AS cc_call_center_id_node_7, cc_rec_start_date AS cc_rec_start_date_node_7, cc_rec_end_date AS cc_rec_end_date_node_7, cc_closed_date_sk AS cc_closed_date_sk_node_7, cc_open_date_sk AS cc_open_date_sk_node_7, cc_name AS cc_name_node_7, cc_class AS cc_class_node_7, cc_employees AS cc_employees_node_7, cc_sq_ft AS cc_sq_ft_node_7, cc_hours AS cc_hours_node_7, cc_manager AS cc_manager_node_7, cc_mkt_id AS cc_mkt_id_node_7, cc_mkt_class AS cc_mkt_class_node_7, cc_mkt_desc AS cc_mkt_desc_node_7, cc_market_manager AS cc_market_manager_node_7, cc_division AS cc_division_node_7, cc_division_name AS cc_division_name_node_7, cc_company AS cc_company_node_7, cc_company_name AS cc_company_name_node_7, cc_street_number AS cc_street_number_node_7, cc_street_name AS cc_street_name_node_7, cc_street_type AS cc_street_type_node_7, cc_suite_number AS cc_suite_number_node_7, cc_city AS cc_city_node_7, cc_county AS cc_county_node_7, cc_state AS cc_state_node_7, cc_zip AS cc_zip_node_7, cc_country AS cc_country_node_7, cc_gmt_offset AS cc_gmt_offset_node_7, cc_tax_percentage AS cc_tax_percentage_node_7, 'hello' AS _c31])
                           +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0