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
    return values.std()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_9 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_8 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_7 = autonode_9.limit(91)
autonode_6 = autonode_8.order_by(col('p_channel_radio_node_8'))
autonode_5 = autonode_7.alias('6Z8DJ')
autonode_4 = autonode_6.alias('ZYBKu')
autonode_3 = autonode_4.join(autonode_5, col('p_cost_node_8') == col('ca_gmt_offset_node_9'))
autonode_2 = autonode_3.group_by(col('p_promo_name_node_8')).select(col('p_cost_node_8').min.alias('p_cost_node_8'))
autonode_1 = autonode_2.add_columns(lit("hello"))
sink = autonode_1.limit(3)
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
LogicalSort(fetch=[3])
+- LogicalProject(p_cost_node_8=[$1], _c1=[_UTF-16LE'hello'])
   +- LogicalAggregate(group=[{7}], EXPR$0=[MIN($5)])
      +- LogicalJoin(condition=[=($5, $30)], joinType=[inner])
         :- LogicalProject(ZYBKu=[AS($0, _UTF-16LE'ZYBKu')], p_promo_id_node_8=[$1], p_start_date_sk_node_8=[$2], p_end_date_sk_node_8=[$3], p_item_sk_node_8=[$4], p_cost_node_8=[$5], p_response_target_node_8=[$6], p_promo_name_node_8=[$7], p_channel_dmail_node_8=[$8], p_channel_email_node_8=[$9], p_channel_catalog_node_8=[$10], p_channel_tv_node_8=[$11], p_channel_radio_node_8=[$12], p_channel_press_node_8=[$13], p_channel_event_node_8=[$14], p_channel_demo_node_8=[$15], p_channel_details_node_8=[$16], p_purpose_node_8=[$17], p_discount_active_node_8=[$18])
         :  +- LogicalSort(sort0=[$12], dir0=[ASC])
         :     +- LogicalProject(p_promo_sk_node_8=[$0], p_promo_id_node_8=[$1], p_start_date_sk_node_8=[$2], p_end_date_sk_node_8=[$3], p_item_sk_node_8=[$4], p_cost_node_8=[$5], p_response_target_node_8=[$6], p_promo_name_node_8=[$7], p_channel_dmail_node_8=[$8], p_channel_email_node_8=[$9], p_channel_catalog_node_8=[$10], p_channel_tv_node_8=[$11], p_channel_radio_node_8=[$12], p_channel_press_node_8=[$13], p_channel_event_node_8=[$14], p_channel_demo_node_8=[$15], p_channel_details_node_8=[$16], p_purpose_node_8=[$17], p_discount_active_node_8=[$18])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])
         +- LogicalProject(6Z8DJ=[AS($0, _UTF-16LE'6Z8DJ')], ca_address_id_node_9=[$1], ca_street_number_node_9=[$2], ca_street_name_node_9=[$3], ca_street_type_node_9=[$4], ca_suite_number_node_9=[$5], ca_city_node_9=[$6], ca_county_node_9=[$7], ca_state_node_9=[$8], ca_zip_node_9=[$9], ca_country_node_9=[$10], ca_gmt_offset_node_9=[$11], ca_location_type_node_9=[$12])
            +- LogicalSort(fetch=[91])
               +- LogicalProject(ca_address_sk_node_9=[$0], ca_address_id_node_9=[$1], ca_street_number_node_9=[$2], ca_street_name_node_9=[$3], ca_street_type_node_9=[$4], ca_suite_number_node_9=[$5], ca_city_node_9=[$6], ca_county_node_9=[$7], ca_state_node_9=[$8], ca_zip_node_9=[$9], ca_country_node_9=[$10], ca_gmt_offset_node_9=[$11], ca_location_type_node_9=[$12])
                  +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS p_cost_node_8, 'hello' AS _c1])
+- Limit(offset=[0], fetch=[3], global=[true])
   +- Exchange(distribution=[single])
      +- Limit(offset=[0], fetch=[3], global=[false])
         +- HashAggregate(isMerge=[true], groupBy=[p_promo_name_node_8], select=[p_promo_name_node_8, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[hash[p_promo_name_node_8]])
               +- LocalHashAggregate(groupBy=[p_promo_name_node_8], select=[p_promo_name_node_8, Partial_MIN(p_cost_node_8) AS min$0])
                  +- Calc(select=[ZYBKu, p_promo_id_node_8, p_start_date_sk_node_8, p_end_date_sk_node_8, p_item_sk_node_8, p_cost_node_8, p_response_target_node_8, p_promo_name_node_8, p_channel_dmail_node_8, p_channel_email_node_8, p_channel_catalog_node_8, p_channel_tv_node_8, p_channel_radio_node_8, p_channel_press_node_8, p_channel_event_node_8, p_channel_demo_node_8, p_channel_details_node_8, p_purpose_node_8, p_discount_active_node_8, 6Z8DJ, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9])
                     +- HashJoin(joinType=[InnerJoin], where=[=(p_cost_node_8, ca_gmt_offset_node_90)], select=[ZYBKu, p_promo_id_node_8, p_start_date_sk_node_8, p_end_date_sk_node_8, p_item_sk_node_8, p_cost_node_8, p_response_target_node_8, p_promo_name_node_8, p_channel_dmail_node_8, p_channel_email_node_8, p_channel_catalog_node_8, p_channel_tv_node_8, p_channel_radio_node_8, p_channel_press_node_8, p_channel_event_node_8, p_channel_demo_node_8, p_channel_details_node_8, p_purpose_node_8, p_discount_active_node_8, 6Z8DJ, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, ca_gmt_offset_node_90], isBroadcast=[true], build=[right])
                        :- Calc(select=[p_promo_sk AS ZYBKu, p_promo_id AS p_promo_id_node_8, p_start_date_sk AS p_start_date_sk_node_8, p_end_date_sk AS p_end_date_sk_node_8, p_item_sk AS p_item_sk_node_8, p_cost AS p_cost_node_8, p_response_target AS p_response_target_node_8, p_promo_name AS p_promo_name_node_8, p_channel_dmail AS p_channel_dmail_node_8, p_channel_email AS p_channel_email_node_8, p_channel_catalog AS p_channel_catalog_node_8, p_channel_tv AS p_channel_tv_node_8, p_channel_radio AS p_channel_radio_node_8, p_channel_press AS p_channel_press_node_8, p_channel_event AS p_channel_event_node_8, p_channel_demo AS p_channel_demo_node_8, p_channel_details AS p_channel_details_node_8, p_purpose AS p_purpose_node_8, p_discount_active AS p_discount_active_node_8])
                        :  +- Sort(orderBy=[p_channel_radio ASC])
                        :     +- Exchange(distribution=[single])
                        :        +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[ca_address_sk AS 6Z8DJ, ca_address_id AS ca_address_id_node_9, ca_street_number AS ca_street_number_node_9, ca_street_name AS ca_street_name_node_9, ca_street_type AS ca_street_type_node_9, ca_suite_number AS ca_suite_number_node_9, ca_city AS ca_city_node_9, ca_county AS ca_county_node_9, ca_state AS ca_state_node_9, ca_zip AS ca_zip_node_9, ca_country AS ca_country_node_9, ca_gmt_offset AS ca_gmt_offset_node_9, ca_location_type AS ca_location_type_node_9, CAST(ca_gmt_offset AS DECIMAL(15, 2)) AS ca_gmt_offset_node_90])
                              +- Limit(offset=[0], fetch=[91], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- Limit(offset=[0], fetch=[91], global=[false])
                                       +- TableSourceScan(table=[[default_catalog, default_database, customer_address, limit=[91]]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS p_cost_node_8, 'hello' AS _c1])
+- Limit(offset=[0], fetch=[3], global=[true])
   +- Exchange(distribution=[single])
      +- Limit(offset=[0], fetch=[3], global=[false])
         +- HashAggregate(isMerge=[true], groupBy=[p_promo_name_node_8], select=[p_promo_name_node_8, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[hash[p_promo_name_node_8]])
               +- LocalHashAggregate(groupBy=[p_promo_name_node_8], select=[p_promo_name_node_8, Partial_MIN(p_cost_node_8) AS min$0])
                  +- Calc(select=[ZYBKu, p_promo_id_node_8, p_start_date_sk_node_8, p_end_date_sk_node_8, p_item_sk_node_8, p_cost_node_8, p_response_target_node_8, p_promo_name_node_8, p_channel_dmail_node_8, p_channel_email_node_8, p_channel_catalog_node_8, p_channel_tv_node_8, p_channel_radio_node_8, p_channel_press_node_8, p_channel_event_node_8, p_channel_demo_node_8, p_channel_details_node_8, p_purpose_node_8, p_discount_active_node_8, 6Z8DJ, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9])
                     +- HashJoin(joinType=[InnerJoin], where=[(p_cost_node_8 = ca_gmt_offset_node_90)], select=[ZYBKu, p_promo_id_node_8, p_start_date_sk_node_8, p_end_date_sk_node_8, p_item_sk_node_8, p_cost_node_8, p_response_target_node_8, p_promo_name_node_8, p_channel_dmail_node_8, p_channel_email_node_8, p_channel_catalog_node_8, p_channel_tv_node_8, p_channel_radio_node_8, p_channel_press_node_8, p_channel_event_node_8, p_channel_demo_node_8, p_channel_details_node_8, p_purpose_node_8, p_discount_active_node_8, 6Z8DJ, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, ca_gmt_offset_node_90], isBroadcast=[true], build=[right])
                        :- Calc(select=[p_promo_sk AS ZYBKu, p_promo_id AS p_promo_id_node_8, p_start_date_sk AS p_start_date_sk_node_8, p_end_date_sk AS p_end_date_sk_node_8, p_item_sk AS p_item_sk_node_8, p_cost AS p_cost_node_8, p_response_target AS p_response_target_node_8, p_promo_name AS p_promo_name_node_8, p_channel_dmail AS p_channel_dmail_node_8, p_channel_email AS p_channel_email_node_8, p_channel_catalog AS p_channel_catalog_node_8, p_channel_tv AS p_channel_tv_node_8, p_channel_radio AS p_channel_radio_node_8, p_channel_press AS p_channel_press_node_8, p_channel_event AS p_channel_event_node_8, p_channel_demo AS p_channel_demo_node_8, p_channel_details AS p_channel_details_node_8, p_purpose AS p_purpose_node_8, p_discount_active AS p_discount_active_node_8])
                        :  +- Sort(orderBy=[p_channel_radio ASC])
                        :     +- Exchange(distribution=[single])
                        :        +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[ca_address_sk AS 6Z8DJ, ca_address_id AS ca_address_id_node_9, ca_street_number AS ca_street_number_node_9, ca_street_name AS ca_street_name_node_9, ca_street_type AS ca_street_type_node_9, ca_suite_number AS ca_suite_number_node_9, ca_city AS ca_city_node_9, ca_county AS ca_county_node_9, ca_state AS ca_state_node_9, ca_zip AS ca_zip_node_9, ca_country AS ca_country_node_9, ca_gmt_offset AS ca_gmt_offset_node_9, ca_location_type AS ca_location_type_node_9, CAST(ca_gmt_offset AS DECIMAL(15, 2)) AS ca_gmt_offset_node_90])
                              +- Limit(offset=[0], fetch=[91], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- Limit(offset=[0], fetch=[91], global=[false])
                                       +- TableSourceScan(table=[[default_catalog, default_database, customer_address, limit=[91]]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o47516268.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#94552741:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[12](input=RelSubset#94552739,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[12]), rel#94552738:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[12](input=RelSubset#94552737,groupBy=p_cost, p_promo_name,select=p_cost, p_promo_name, Partial_MIN(p_cost) AS min$0)]
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