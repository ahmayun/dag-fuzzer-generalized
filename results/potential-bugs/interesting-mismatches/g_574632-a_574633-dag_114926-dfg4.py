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
    return values.var()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_9 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_12 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_11 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_6 = autonode_9.join(autonode_10, col('ca_gmt_offset_node_9') == col('sr_return_ship_cost_node_10'))
autonode_8 = autonode_12.order_by(col('inv_warehouse_sk_node_12'))
autonode_7 = autonode_11.limit(42)
autonode_4 = autonode_6.alias('myrTs')
autonode_5 = autonode_7.join(autonode_8, col('inv_date_sk_node_12') == col('sm_ship_mode_sk_node_11'))
autonode_2 = autonode_4.distinct()
autonode_3 = autonode_5.group_by(col('sm_code_node_11')).select(col('sm_contract_node_11').min.alias('sm_contract_node_11'))
autonode_1 = autonode_2.join(autonode_3, col('sm_contract_node_11') == col('ca_state_node_9'))
sink = autonode_1.alias('SS5bf')
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
      "error_message": "An error occurred while calling o313482488.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#631518812:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[2](input=RelSubset#631518810,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[2]), rel#631518809:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[2](input=RelSubset#631518808,groupBy=inv_date_sk,select=inv_date_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (2) must be less than size (1)
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
LogicalProject(SS5bf=[AS($0, _UTF-16LE'SS5bf')], ca_address_id_node_9=[$1], ca_street_number_node_9=[$2], ca_street_name_node_9=[$3], ca_street_type_node_9=[$4], ca_suite_number_node_9=[$5], ca_city_node_9=[$6], ca_county_node_9=[$7], ca_state_node_9=[$8], ca_zip_node_9=[$9], ca_country_node_9=[$10], ca_gmt_offset_node_9=[$11], ca_location_type_node_9=[$12], sr_returned_date_sk_node_10=[$13], sr_return_time_sk_node_10=[$14], sr_item_sk_node_10=[$15], sr_customer_sk_node_10=[$16], sr_cdemo_sk_node_10=[$17], sr_hdemo_sk_node_10=[$18], sr_addr_sk_node_10=[$19], sr_store_sk_node_10=[$20], sr_reason_sk_node_10=[$21], sr_ticket_number_node_10=[$22], sr_return_quantity_node_10=[$23], sr_return_amt_node_10=[$24], sr_return_tax_node_10=[$25], sr_return_amt_inc_tax_node_10=[$26], sr_fee_node_10=[$27], sr_return_ship_cost_node_10=[$28], sr_refunded_cash_node_10=[$29], sr_reversed_charge_node_10=[$30], sr_store_credit_node_10=[$31], sr_net_loss_node_10=[$32], sm_contract_node_11=[$33])
+- LogicalJoin(condition=[=($33, $8)], joinType=[inner])
   :- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}])
   :  +- LogicalProject(myrTs=[AS($0, _UTF-16LE'myrTs')], ca_address_id_node_9=[$1], ca_street_number_node_9=[$2], ca_street_name_node_9=[$3], ca_street_type_node_9=[$4], ca_suite_number_node_9=[$5], ca_city_node_9=[$6], ca_county_node_9=[$7], ca_state_node_9=[$8], ca_zip_node_9=[$9], ca_country_node_9=[$10], ca_gmt_offset_node_9=[$11], ca_location_type_node_9=[$12], sr_returned_date_sk_node_10=[$13], sr_return_time_sk_node_10=[$14], sr_item_sk_node_10=[$15], sr_customer_sk_node_10=[$16], sr_cdemo_sk_node_10=[$17], sr_hdemo_sk_node_10=[$18], sr_addr_sk_node_10=[$19], sr_store_sk_node_10=[$20], sr_reason_sk_node_10=[$21], sr_ticket_number_node_10=[$22], sr_return_quantity_node_10=[$23], sr_return_amt_node_10=[$24], sr_return_tax_node_10=[$25], sr_return_amt_inc_tax_node_10=[$26], sr_fee_node_10=[$27], sr_return_ship_cost_node_10=[$28], sr_refunded_cash_node_10=[$29], sr_reversed_charge_node_10=[$30], sr_store_credit_node_10=[$31], sr_net_loss_node_10=[$32])
   :     +- LogicalJoin(condition=[=($11, $28)], joinType=[inner])
   :        :- LogicalProject(ca_address_sk_node_9=[$0], ca_address_id_node_9=[$1], ca_street_number_node_9=[$2], ca_street_name_node_9=[$3], ca_street_type_node_9=[$4], ca_suite_number_node_9=[$5], ca_city_node_9=[$6], ca_county_node_9=[$7], ca_state_node_9=[$8], ca_zip_node_9=[$9], ca_country_node_9=[$10], ca_gmt_offset_node_9=[$11], ca_location_type_node_9=[$12])
   :        :  +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
   :        +- LogicalProject(sr_returned_date_sk_node_10=[$0], sr_return_time_sk_node_10=[$1], sr_item_sk_node_10=[$2], sr_customer_sk_node_10=[$3], sr_cdemo_sk_node_10=[$4], sr_hdemo_sk_node_10=[$5], sr_addr_sk_node_10=[$6], sr_store_sk_node_10=[$7], sr_reason_sk_node_10=[$8], sr_ticket_number_node_10=[$9], sr_return_quantity_node_10=[$10], sr_return_amt_node_10=[$11], sr_return_tax_node_10=[$12], sr_return_amt_inc_tax_node_10=[$13], sr_fee_node_10=[$14], sr_return_ship_cost_node_10=[$15], sr_refunded_cash_node_10=[$16], sr_reversed_charge_node_10=[$17], sr_store_credit_node_10=[$18], sr_net_loss_node_10=[$19])
   :           +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
   +- LogicalProject(sm_contract_node_11=[$1])
      +- LogicalAggregate(group=[{3}], EXPR$0=[MIN($5)])
         +- LogicalJoin(condition=[=($6, $0)], joinType=[inner])
            :- LogicalSort(fetch=[42])
            :  +- LogicalProject(sm_ship_mode_sk_node_11=[$0], sm_ship_mode_id_node_11=[$1], sm_type_node_11=[$2], sm_code_node_11=[$3], sm_carrier_node_11=[$4], sm_contract_node_11=[$5])
            :     +- LogicalTableScan(table=[[default_catalog, default_database, ship_mode]])
            +- LogicalSort(sort0=[$2], dir0=[ASC])
               +- LogicalProject(inv_date_sk_node_12=[$0], inv_item_sk_node_12=[$1], inv_warehouse_sk_node_12=[$2], inv_quantity_on_hand_node_12=[$3])
                  +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])

== Optimized Physical Plan ==
Calc(select=[myrTs AS SS5bf, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10, sm_contract_node_11])
+- NestedLoopJoin(joinType=[InnerJoin], where=[=(sm_contract_node_11, ca_state_node_9)], select=[myrTs, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10, sm_contract_node_11], build=[right])
   :- HashAggregate(isMerge=[false], groupBy=[myrTs, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10], select=[myrTs, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10])
   :  +- Calc(select=[ca_address_sk_node_9 AS myrTs, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, sr_returned_date_sk AS sr_returned_date_sk_node_10, sr_return_time_sk AS sr_return_time_sk_node_10, sr_item_sk AS sr_item_sk_node_10, sr_customer_sk AS sr_customer_sk_node_10, sr_cdemo_sk AS sr_cdemo_sk_node_10, sr_hdemo_sk AS sr_hdemo_sk_node_10, sr_addr_sk AS sr_addr_sk_node_10, sr_store_sk AS sr_store_sk_node_10, sr_reason_sk AS sr_reason_sk_node_10, sr_ticket_number AS sr_ticket_number_node_10, sr_return_quantity AS sr_return_quantity_node_10, sr_return_amt AS sr_return_amt_node_10, sr_return_tax AS sr_return_tax_node_10, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_10, sr_fee AS sr_fee_node_10, sr_return_ship_cost AS sr_return_ship_cost_node_10, sr_refunded_cash AS sr_refunded_cash_node_10, sr_reversed_charge AS sr_reversed_charge_node_10, sr_store_credit AS sr_store_credit_node_10, sr_net_loss AS sr_net_loss_node_10])
   :     +- HashJoin(joinType=[InnerJoin], where=[=(ca_gmt_offset_node_90, sr_return_ship_cost)], select=[ca_address_sk_node_9, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, ca_gmt_offset_node_90, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], build=[left])
   :        :- Exchange(distribution=[hash[ca_gmt_offset_node_90]])
   :        :  +- Calc(select=[ca_address_sk AS ca_address_sk_node_9, ca_address_id AS ca_address_id_node_9, ca_street_number AS ca_street_number_node_9, ca_street_name AS ca_street_name_node_9, ca_street_type AS ca_street_type_node_9, ca_suite_number AS ca_suite_number_node_9, ca_city AS ca_city_node_9, ca_county AS ca_county_node_9, ca_state AS ca_state_node_9, ca_zip AS ca_zip_node_9, ca_country AS ca_country_node_9, ca_gmt_offset AS ca_gmt_offset_node_9, ca_location_type AS ca_location_type_node_9, CAST(ca_gmt_offset AS DECIMAL(7, 2)) AS ca_gmt_offset_node_90])
   :        :     +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
   :        +- Exchange(distribution=[hash[sr_return_ship_cost]])
   :           +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
   +- Exchange(distribution=[broadcast])
      +- Calc(select=[EXPR$0 AS sm_contract_node_11])
         +- SortAggregate(isMerge=[true], groupBy=[sm_code], select=[sm_code, Final_MIN(min$0) AS EXPR$0])
            +- Sort(orderBy=[sm_code ASC])
               +- Exchange(distribution=[hash[sm_code]])
                  +- LocalSortAggregate(groupBy=[sm_code], select=[sm_code, Partial_MIN(sm_contract) AS min$0])
                     +- Sort(orderBy=[sm_code ASC])
                        +- NestedLoopJoin(joinType=[InnerJoin], where=[=(inv_date_sk, sm_ship_mode_sk)], select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand], build=[right])
                           :- Limit(offset=[0], fetch=[42], global=[true])
                           :  +- Exchange(distribution=[single])
                           :     +- Limit(offset=[0], fetch=[42], global=[false])
                           :        +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
                           +- Exchange(distribution=[broadcast])
                              +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[false])
                                       +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

== Optimized Execution Plan ==
Calc(select=[myrTs AS SS5bf, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10, sm_contract_node_11])
+- NestedLoopJoin(joinType=[InnerJoin], where=[(sm_contract_node_11 = ca_state_node_9)], select=[myrTs, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10, sm_contract_node_11], build=[right])
   :- HashAggregate(isMerge=[false], groupBy=[myrTs, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10], select=[myrTs, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10])
   :  +- Exchange(distribution=[keep_input_as_is[hash[myrTs, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, sr_returned_date_sk_node_10, sr_return_time_sk_node_10, sr_item_sk_node_10, sr_customer_sk_node_10, sr_cdemo_sk_node_10, sr_hdemo_sk_node_10, sr_addr_sk_node_10, sr_store_sk_node_10, sr_reason_sk_node_10, sr_ticket_number_node_10, sr_return_quantity_node_10, sr_return_amt_node_10, sr_return_tax_node_10, sr_return_amt_inc_tax_node_10, sr_fee_node_10, sr_return_ship_cost_node_10, sr_refunded_cash_node_10, sr_reversed_charge_node_10, sr_store_credit_node_10, sr_net_loss_node_10]]])
   :     +- Calc(select=[ca_address_sk_node_9 AS myrTs, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, sr_returned_date_sk AS sr_returned_date_sk_node_10, sr_return_time_sk AS sr_return_time_sk_node_10, sr_item_sk AS sr_item_sk_node_10, sr_customer_sk AS sr_customer_sk_node_10, sr_cdemo_sk AS sr_cdemo_sk_node_10, sr_hdemo_sk AS sr_hdemo_sk_node_10, sr_addr_sk AS sr_addr_sk_node_10, sr_store_sk AS sr_store_sk_node_10, sr_reason_sk AS sr_reason_sk_node_10, sr_ticket_number AS sr_ticket_number_node_10, sr_return_quantity AS sr_return_quantity_node_10, sr_return_amt AS sr_return_amt_node_10, sr_return_tax AS sr_return_tax_node_10, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_10, sr_fee AS sr_fee_node_10, sr_return_ship_cost AS sr_return_ship_cost_node_10, sr_refunded_cash AS sr_refunded_cash_node_10, sr_reversed_charge AS sr_reversed_charge_node_10, sr_store_credit AS sr_store_credit_node_10, sr_net_loss AS sr_net_loss_node_10])
   :        +- Exchange(distribution=[keep_input_as_is[hash[ca_address_sk_node_9, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, ca_gmt_offset_node_90, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit]]])
   :           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ca_gmt_offset_node_90 = sr_return_ship_cost)], select=[ca_address_sk_node_9, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, ca_gmt_offset_node_90, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], build=[left])
   :              :- Exchange(distribution=[hash[ca_gmt_offset_node_90]])
   :              :  +- Calc(select=[ca_address_sk AS ca_address_sk_node_9, ca_address_id AS ca_address_id_node_9, ca_street_number AS ca_street_number_node_9, ca_street_name AS ca_street_name_node_9, ca_street_type AS ca_street_type_node_9, ca_suite_number AS ca_suite_number_node_9, ca_city AS ca_city_node_9, ca_county AS ca_county_node_9, ca_state AS ca_state_node_9, ca_zip AS ca_zip_node_9, ca_country AS ca_country_node_9, ca_gmt_offset AS ca_gmt_offset_node_9, ca_location_type AS ca_location_type_node_9, CAST(ca_gmt_offset AS DECIMAL(7, 2)) AS ca_gmt_offset_node_90])
   :              :     +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
   :              +- Exchange(distribution=[hash[sr_return_ship_cost]])
   :                 +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
   +- Exchange(distribution=[broadcast])
      +- Calc(select=[EXPR$0 AS sm_contract_node_11])
         +- SortAggregate(isMerge=[true], groupBy=[sm_code], select=[sm_code, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[forward])
               +- Sort(orderBy=[sm_code ASC])
                  +- Exchange(distribution=[hash[sm_code]])
                     +- LocalSortAggregate(groupBy=[sm_code], select=[sm_code, Partial_MIN(sm_contract) AS min$0])
                        +- Exchange(distribution=[forward])
                           +- Sort(orderBy=[sm_code ASC])
                              +- NestedLoopJoin(joinType=[InnerJoin], where=[(inv_date_sk = sm_ship_mode_sk)], select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand], build=[right])
                                 :- Limit(offset=[0], fetch=[42], global=[true])
                                 :  +- Exchange(distribution=[single])
                                 :     +- Limit(offset=[0], fetch=[42], global=[false])
                                 :        +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
                                 +- Exchange(distribution=[broadcast])
                                    +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[true])
                                       +- Exchange(distribution=[single])
                                          +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[false])
                                             +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0