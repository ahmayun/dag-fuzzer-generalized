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
    return values.sum()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_9 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_8 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_11 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_6 = autonode_10.filter(col('p_channel_dmail_node_10').char_length > 5)
autonode_5 = autonode_8.join(autonode_9, col('cd_dep_count_node_9') == col('ca_address_sk_node_8'))
autonode_7 = autonode_11.order_by(col('cs_wholesale_cost_node_11'))
autonode_3 = autonode_5.limit(45)
autonode_4 = autonode_6.join(autonode_7, col('p_cost_node_10') == col('cs_ext_list_price_node_11'))
autonode_2 = autonode_3.join(autonode_4, col('p_channel_radio_node_10') == col('ca_state_node_8'))
autonode_1 = autonode_2.group_by(col('p_channel_event_node_10')).select(col('ca_address_sk_node_8').min.alias('ca_address_sk_node_8'))
sink = autonode_1.alias('sorkS')
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
      "error_message": "An error occurred while calling o179655432.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#363350936:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[19](input=RelSubset#363350934,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[19]), rel#363350933:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[19](input=RelSubset#363350932,groupBy=cs_ext_list_price_node_110,select=cs_ext_list_price_node_110)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (19) must be less than size (1)
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
LogicalProject(sorkS=[AS($1, _UTF-16LE'sorkS')])
+- LogicalAggregate(group=[{36}], EXPR$0=[MIN($0)])
   +- LogicalJoin(condition=[=($34, $8)], joinType=[inner])
      :- LogicalSort(fetch=[45])
      :  +- LogicalJoin(condition=[=($19, $0)], joinType=[inner])
      :     :- LogicalProject(ca_address_sk_node_8=[$0], ca_address_id_node_8=[$1], ca_street_number_node_8=[$2], ca_street_name_node_8=[$3], ca_street_type_node_8=[$4], ca_suite_number_node_8=[$5], ca_city_node_8=[$6], ca_county_node_8=[$7], ca_state_node_8=[$8], ca_zip_node_8=[$9], ca_country_node_8=[$10], ca_gmt_offset_node_8=[$11], ca_location_type_node_8=[$12])
      :     :  +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
      :     +- LogicalProject(cd_demo_sk_node_9=[$0], cd_gender_node_9=[$1], cd_marital_status_node_9=[$2], cd_education_status_node_9=[$3], cd_purchase_estimate_node_9=[$4], cd_credit_rating_node_9=[$5], cd_dep_count_node_9=[$6], cd_dep_employed_count_node_9=[$7], cd_dep_college_count_node_9=[$8])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
      +- LogicalJoin(condition=[=($5, $44)], joinType=[inner])
         :- LogicalFilter(condition=[>(CHAR_LENGTH($8), 5)])
         :  +- LogicalProject(p_promo_sk_node_10=[$0], p_promo_id_node_10=[$1], p_start_date_sk_node_10=[$2], p_end_date_sk_node_10=[$3], p_item_sk_node_10=[$4], p_cost_node_10=[$5], p_response_target_node_10=[$6], p_promo_name_node_10=[$7], p_channel_dmail_node_10=[$8], p_channel_email_node_10=[$9], p_channel_catalog_node_10=[$10], p_channel_tv_node_10=[$11], p_channel_radio_node_10=[$12], p_channel_press_node_10=[$13], p_channel_event_node_10=[$14], p_channel_demo_node_10=[$15], p_channel_details_node_10=[$16], p_purpose_node_10=[$17], p_discount_active_node_10=[$18])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])
         +- LogicalSort(sort0=[$19], dir0=[ASC])
            +- LogicalProject(cs_sold_date_sk_node_11=[$0], cs_sold_time_sk_node_11=[$1], cs_ship_date_sk_node_11=[$2], cs_bill_customer_sk_node_11=[$3], cs_bill_cdemo_sk_node_11=[$4], cs_bill_hdemo_sk_node_11=[$5], cs_bill_addr_sk_node_11=[$6], cs_ship_customer_sk_node_11=[$7], cs_ship_cdemo_sk_node_11=[$8], cs_ship_hdemo_sk_node_11=[$9], cs_ship_addr_sk_node_11=[$10], cs_call_center_sk_node_11=[$11], cs_catalog_page_sk_node_11=[$12], cs_ship_mode_sk_node_11=[$13], cs_warehouse_sk_node_11=[$14], cs_item_sk_node_11=[$15], cs_promo_sk_node_11=[$16], cs_order_number_node_11=[$17], cs_quantity_node_11=[$18], cs_wholesale_cost_node_11=[$19], cs_list_price_node_11=[$20], cs_sales_price_node_11=[$21], cs_ext_discount_amt_node_11=[$22], cs_ext_sales_price_node_11=[$23], cs_ext_wholesale_cost_node_11=[$24], cs_ext_list_price_node_11=[$25], cs_ext_tax_node_11=[$26], cs_coupon_amt_node_11=[$27], cs_ext_ship_cost_node_11=[$28], cs_net_paid_node_11=[$29], cs_net_paid_inc_tax_node_11=[$30], cs_net_paid_inc_ship_node_11=[$31], cs_net_paid_inc_ship_tax_node_11=[$32], cs_net_profit_node_11=[$33])
               +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS sorkS])
+- HashAggregate(isMerge=[false], groupBy=[p_channel_event_node_10], select=[p_channel_event_node_10, MIN(ca_address_sk) AS EXPR$0])
   +- Exchange(distribution=[hash[p_channel_event_node_10]])
      +- HashJoin(joinType=[InnerJoin], where=[=(p_channel_radio_node_10, ca_state)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, p_promo_sk_node_10, p_promo_id_node_10, p_start_date_sk_node_10, p_end_date_sk_node_10, p_item_sk_node_10, p_cost_node_10, p_response_target_node_10, p_promo_name_node_10, p_channel_dmail_node_10, p_channel_email_node_10, p_channel_catalog_node_10, p_channel_tv_node_10, p_channel_radio_node_10, p_channel_press_node_10, p_channel_event_node_10, p_channel_demo_node_10, p_channel_details_node_10, p_purpose_node_10, p_discount_active_node_10, cs_sold_date_sk_node_11, cs_sold_time_sk_node_11, cs_ship_date_sk_node_11, cs_bill_customer_sk_node_11, cs_bill_cdemo_sk_node_11, cs_bill_hdemo_sk_node_11, cs_bill_addr_sk_node_11, cs_ship_customer_sk_node_11, cs_ship_cdemo_sk_node_11, cs_ship_hdemo_sk_node_11, cs_ship_addr_sk_node_11, cs_call_center_sk_node_11, cs_catalog_page_sk_node_11, cs_ship_mode_sk_node_11, cs_warehouse_sk_node_11, cs_item_sk_node_11, cs_promo_sk_node_11, cs_order_number_node_11, cs_quantity_node_11, cs_wholesale_cost_node_11, cs_list_price_node_11, cs_sales_price_node_11, cs_ext_discount_amt_node_11, cs_ext_sales_price_node_11, cs_ext_wholesale_cost_node_11, cs_ext_list_price_node_11, cs_ext_tax_node_11, cs_coupon_amt_node_11, cs_ext_ship_cost_node_11, cs_net_paid_node_11, cs_net_paid_inc_tax_node_11, cs_net_paid_inc_ship_node_11, cs_net_paid_inc_ship_tax_node_11, cs_net_profit_node_11], isBroadcast=[true], build=[right])
         :- Limit(offset=[0], fetch=[45], global=[true])
         :  +- Exchange(distribution=[single])
         :     +- Limit(offset=[0], fetch=[45], global=[false])
         :        +- HashJoin(joinType=[InnerJoin], where=[=(cd_dep_count, ca_address_sk)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[left])
         :           :- Exchange(distribution=[hash[ca_address_sk]])
         :           :  +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
         :           +- Exchange(distribution=[hash[cd_dep_count]])
         :              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[p_promo_sk AS p_promo_sk_node_10, p_promo_id AS p_promo_id_node_10, p_start_date_sk AS p_start_date_sk_node_10, p_end_date_sk AS p_end_date_sk_node_10, p_item_sk AS p_item_sk_node_10, p_cost AS p_cost_node_10, p_response_target AS p_response_target_node_10, p_promo_name AS p_promo_name_node_10, p_channel_dmail AS p_channel_dmail_node_10, p_channel_email AS p_channel_email_node_10, p_channel_catalog AS p_channel_catalog_node_10, p_channel_tv AS p_channel_tv_node_10, p_channel_radio AS p_channel_radio_node_10, p_channel_press AS p_channel_press_node_10, p_channel_event AS p_channel_event_node_10, p_channel_demo AS p_channel_demo_node_10, p_channel_details AS p_channel_details_node_10, p_purpose AS p_purpose_node_10, p_discount_active AS p_discount_active_node_10, cs_sold_date_sk_node_11, cs_sold_time_sk_node_11, cs_ship_date_sk_node_11, cs_bill_customer_sk_node_11, cs_bill_cdemo_sk_node_11, cs_bill_hdemo_sk_node_11, cs_bill_addr_sk_node_11, cs_ship_customer_sk_node_11, cs_ship_cdemo_sk_node_11, cs_ship_hdemo_sk_node_11, cs_ship_addr_sk_node_11, cs_call_center_sk_node_11, cs_catalog_page_sk_node_11, cs_ship_mode_sk_node_11, cs_warehouse_sk_node_11, cs_item_sk_node_11, cs_promo_sk_node_11, cs_order_number_node_11, cs_quantity_node_11, cs_wholesale_cost_node_11, cs_list_price_node_11, cs_sales_price_node_11, cs_ext_discount_amt_node_11, cs_ext_sales_price_node_11, cs_ext_wholesale_cost_node_11, cs_ext_list_price_node_11, cs_ext_tax_node_11, cs_coupon_amt_node_11, cs_ext_ship_cost_node_11, cs_net_paid_node_11, cs_net_paid_inc_tax_node_11, cs_net_paid_inc_ship_node_11, cs_net_paid_inc_ship_tax_node_11, cs_net_profit_node_11])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(p_cost, cs_ext_list_price_node_110)], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, cs_sold_date_sk_node_11, cs_sold_time_sk_node_11, cs_ship_date_sk_node_11, cs_bill_customer_sk_node_11, cs_bill_cdemo_sk_node_11, cs_bill_hdemo_sk_node_11, cs_bill_addr_sk_node_11, cs_ship_customer_sk_node_11, cs_ship_cdemo_sk_node_11, cs_ship_hdemo_sk_node_11, cs_ship_addr_sk_node_11, cs_call_center_sk_node_11, cs_catalog_page_sk_node_11, cs_ship_mode_sk_node_11, cs_warehouse_sk_node_11, cs_item_sk_node_11, cs_promo_sk_node_11, cs_order_number_node_11, cs_quantity_node_11, cs_wholesale_cost_node_11, cs_list_price_node_11, cs_sales_price_node_11, cs_ext_discount_amt_node_11, cs_ext_sales_price_node_11, cs_ext_wholesale_cost_node_11, cs_ext_list_price_node_11, cs_ext_tax_node_11, cs_coupon_amt_node_11, cs_ext_ship_cost_node_11, cs_net_paid_node_11, cs_net_paid_inc_tax_node_11, cs_net_paid_inc_ship_node_11, cs_net_paid_inc_ship_tax_node_11, cs_net_profit_node_11, cs_ext_list_price_node_110], build=[right])
                  :- Calc(select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], where=[>(CHAR_LENGTH(p_channel_dmail), 5)])
                  :  +- TableSourceScan(table=[[default_catalog, default_database, promotion, filter=[>(CHAR_LENGTH(p_channel_dmail), 5)]]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                  +- Exchange(distribution=[broadcast])
                     +- Calc(select=[cs_sold_date_sk AS cs_sold_date_sk_node_11, cs_sold_time_sk AS cs_sold_time_sk_node_11, cs_ship_date_sk AS cs_ship_date_sk_node_11, cs_bill_customer_sk AS cs_bill_customer_sk_node_11, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_11, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_11, cs_bill_addr_sk AS cs_bill_addr_sk_node_11, cs_ship_customer_sk AS cs_ship_customer_sk_node_11, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_11, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_11, cs_ship_addr_sk AS cs_ship_addr_sk_node_11, cs_call_center_sk AS cs_call_center_sk_node_11, cs_catalog_page_sk AS cs_catalog_page_sk_node_11, cs_ship_mode_sk AS cs_ship_mode_sk_node_11, cs_warehouse_sk AS cs_warehouse_sk_node_11, cs_item_sk AS cs_item_sk_node_11, cs_promo_sk AS cs_promo_sk_node_11, cs_order_number AS cs_order_number_node_11, cs_quantity AS cs_quantity_node_11, cs_wholesale_cost AS cs_wholesale_cost_node_11, cs_list_price AS cs_list_price_node_11, cs_sales_price AS cs_sales_price_node_11, cs_ext_discount_amt AS cs_ext_discount_amt_node_11, cs_ext_sales_price AS cs_ext_sales_price_node_11, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_11, cs_ext_list_price AS cs_ext_list_price_node_11, cs_ext_tax AS cs_ext_tax_node_11, cs_coupon_amt AS cs_coupon_amt_node_11, cs_ext_ship_cost AS cs_ext_ship_cost_node_11, cs_net_paid AS cs_net_paid_node_11, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_11, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_11, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_11, cs_net_profit AS cs_net_profit_node_11, CAST(cs_ext_list_price AS DECIMAL(15, 2)) AS cs_ext_list_price_node_110])
                        +- SortLimit(orderBy=[cs_wholesale_cost ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[cs_wholesale_cost ASC], offset=[0], fetch=[1], global=[false])
                                 +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS sorkS])
+- HashAggregate(isMerge=[false], groupBy=[p_channel_event_node_10], select=[p_channel_event_node_10, MIN(ca_address_sk) AS EXPR$0])
   +- Exchange(distribution=[hash[p_channel_event_node_10]])
      +- HashJoin(joinType=[InnerJoin], where=[(p_channel_radio_node_10 = ca_state)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, p_promo_sk_node_10, p_promo_id_node_10, p_start_date_sk_node_10, p_end_date_sk_node_10, p_item_sk_node_10, p_cost_node_10, p_response_target_node_10, p_promo_name_node_10, p_channel_dmail_node_10, p_channel_email_node_10, p_channel_catalog_node_10, p_channel_tv_node_10, p_channel_radio_node_10, p_channel_press_node_10, p_channel_event_node_10, p_channel_demo_node_10, p_channel_details_node_10, p_purpose_node_10, p_discount_active_node_10, cs_sold_date_sk_node_11, cs_sold_time_sk_node_11, cs_ship_date_sk_node_11, cs_bill_customer_sk_node_11, cs_bill_cdemo_sk_node_11, cs_bill_hdemo_sk_node_11, cs_bill_addr_sk_node_11, cs_ship_customer_sk_node_11, cs_ship_cdemo_sk_node_11, cs_ship_hdemo_sk_node_11, cs_ship_addr_sk_node_11, cs_call_center_sk_node_11, cs_catalog_page_sk_node_11, cs_ship_mode_sk_node_11, cs_warehouse_sk_node_11, cs_item_sk_node_11, cs_promo_sk_node_11, cs_order_number_node_11, cs_quantity_node_11, cs_wholesale_cost_node_11, cs_list_price_node_11, cs_sales_price_node_11, cs_ext_discount_amt_node_11, cs_ext_sales_price_node_11, cs_ext_wholesale_cost_node_11, cs_ext_list_price_node_11, cs_ext_tax_node_11, cs_coupon_amt_node_11, cs_ext_ship_cost_node_11, cs_net_paid_node_11, cs_net_paid_inc_tax_node_11, cs_net_paid_inc_ship_node_11, cs_net_paid_inc_ship_tax_node_11, cs_net_profit_node_11], isBroadcast=[true], build=[right])
         :- Limit(offset=[0], fetch=[45], global=[true])
         :  +- Exchange(distribution=[single])
         :     +- Limit(offset=[0], fetch=[45], global=[false])
         :        +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(cd_dep_count = ca_address_sk)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[left])
         :           :- Exchange(distribution=[hash[ca_address_sk]])
         :           :  +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
         :           +- Exchange(distribution=[hash[cd_dep_count]])
         :              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[p_promo_sk AS p_promo_sk_node_10, p_promo_id AS p_promo_id_node_10, p_start_date_sk AS p_start_date_sk_node_10, p_end_date_sk AS p_end_date_sk_node_10, p_item_sk AS p_item_sk_node_10, p_cost AS p_cost_node_10, p_response_target AS p_response_target_node_10, p_promo_name AS p_promo_name_node_10, p_channel_dmail AS p_channel_dmail_node_10, p_channel_email AS p_channel_email_node_10, p_channel_catalog AS p_channel_catalog_node_10, p_channel_tv AS p_channel_tv_node_10, p_channel_radio AS p_channel_radio_node_10, p_channel_press AS p_channel_press_node_10, p_channel_event AS p_channel_event_node_10, p_channel_demo AS p_channel_demo_node_10, p_channel_details AS p_channel_details_node_10, p_purpose AS p_purpose_node_10, p_discount_active AS p_discount_active_node_10, cs_sold_date_sk_node_11, cs_sold_time_sk_node_11, cs_ship_date_sk_node_11, cs_bill_customer_sk_node_11, cs_bill_cdemo_sk_node_11, cs_bill_hdemo_sk_node_11, cs_bill_addr_sk_node_11, cs_ship_customer_sk_node_11, cs_ship_cdemo_sk_node_11, cs_ship_hdemo_sk_node_11, cs_ship_addr_sk_node_11, cs_call_center_sk_node_11, cs_catalog_page_sk_node_11, cs_ship_mode_sk_node_11, cs_warehouse_sk_node_11, cs_item_sk_node_11, cs_promo_sk_node_11, cs_order_number_node_11, cs_quantity_node_11, cs_wholesale_cost_node_11, cs_list_price_node_11, cs_sales_price_node_11, cs_ext_discount_amt_node_11, cs_ext_sales_price_node_11, cs_ext_wholesale_cost_node_11, cs_ext_list_price_node_11, cs_ext_tax_node_11, cs_coupon_amt_node_11, cs_ext_ship_cost_node_11, cs_net_paid_node_11, cs_net_paid_inc_tax_node_11, cs_net_paid_inc_ship_node_11, cs_net_paid_inc_ship_tax_node_11, cs_net_profit_node_11])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[(p_cost = cs_ext_list_price_node_110)], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, cs_sold_date_sk_node_11, cs_sold_time_sk_node_11, cs_ship_date_sk_node_11, cs_bill_customer_sk_node_11, cs_bill_cdemo_sk_node_11, cs_bill_hdemo_sk_node_11, cs_bill_addr_sk_node_11, cs_ship_customer_sk_node_11, cs_ship_cdemo_sk_node_11, cs_ship_hdemo_sk_node_11, cs_ship_addr_sk_node_11, cs_call_center_sk_node_11, cs_catalog_page_sk_node_11, cs_ship_mode_sk_node_11, cs_warehouse_sk_node_11, cs_item_sk_node_11, cs_promo_sk_node_11, cs_order_number_node_11, cs_quantity_node_11, cs_wholesale_cost_node_11, cs_list_price_node_11, cs_sales_price_node_11, cs_ext_discount_amt_node_11, cs_ext_sales_price_node_11, cs_ext_wholesale_cost_node_11, cs_ext_list_price_node_11, cs_ext_tax_node_11, cs_coupon_amt_node_11, cs_ext_ship_cost_node_11, cs_net_paid_node_11, cs_net_paid_inc_tax_node_11, cs_net_paid_inc_ship_node_11, cs_net_paid_inc_ship_tax_node_11, cs_net_profit_node_11, cs_ext_list_price_node_110], build=[right])
                  :- Calc(select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], where=[(CHAR_LENGTH(p_channel_dmail) > 5)])
                  :  +- TableSourceScan(table=[[default_catalog, default_database, promotion, filter=[>(CHAR_LENGTH(p_channel_dmail), 5)]]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                  +- Exchange(distribution=[broadcast])
                     +- Calc(select=[cs_sold_date_sk AS cs_sold_date_sk_node_11, cs_sold_time_sk AS cs_sold_time_sk_node_11, cs_ship_date_sk AS cs_ship_date_sk_node_11, cs_bill_customer_sk AS cs_bill_customer_sk_node_11, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_11, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_11, cs_bill_addr_sk AS cs_bill_addr_sk_node_11, cs_ship_customer_sk AS cs_ship_customer_sk_node_11, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_11, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_11, cs_ship_addr_sk AS cs_ship_addr_sk_node_11, cs_call_center_sk AS cs_call_center_sk_node_11, cs_catalog_page_sk AS cs_catalog_page_sk_node_11, cs_ship_mode_sk AS cs_ship_mode_sk_node_11, cs_warehouse_sk AS cs_warehouse_sk_node_11, cs_item_sk AS cs_item_sk_node_11, cs_promo_sk AS cs_promo_sk_node_11, cs_order_number AS cs_order_number_node_11, cs_quantity AS cs_quantity_node_11, cs_wholesale_cost AS cs_wholesale_cost_node_11, cs_list_price AS cs_list_price_node_11, cs_sales_price AS cs_sales_price_node_11, cs_ext_discount_amt AS cs_ext_discount_amt_node_11, cs_ext_sales_price AS cs_ext_sales_price_node_11, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_11, cs_ext_list_price AS cs_ext_list_price_node_11, cs_ext_tax AS cs_ext_tax_node_11, cs_coupon_amt AS cs_coupon_amt_node_11, cs_ext_ship_cost AS cs_ext_ship_cost_node_11, cs_net_paid AS cs_net_paid_node_11, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_11, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_11, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_11, cs_net_profit AS cs_net_profit_node_11, CAST(cs_ext_list_price AS DECIMAL(15, 2)) AS cs_ext_list_price_node_110])
                        +- SortLimit(orderBy=[cs_wholesale_cost ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[cs_wholesale_cost ASC], offset=[0], fetch=[1], global=[false])
                                 +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0