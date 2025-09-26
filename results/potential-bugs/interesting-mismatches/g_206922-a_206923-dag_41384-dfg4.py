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
    import numpy as np
    return np.exp(np.log(values[values > 0]).mean()) if (values > 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_4 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_4") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_5 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_2 = autonode_4.alias('AptYm')
autonode_3 = autonode_5.order_by(col('ca_city_node_5'))
autonode_1 = autonode_2.join(autonode_3, col('sr_return_amt_node_4') == col('ca_gmt_offset_node_5'))
sink = autonode_1.group_by(col('ca_county_node_5')).select(col('sr_store_credit_node_4').min.alias('sr_store_credit_node_4'))
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
LogicalProject(sr_store_credit_node_4=[$1])
+- LogicalAggregate(group=[{27}], EXPR$0=[MIN($18)])
   +- LogicalJoin(condition=[=($11, $31)], joinType=[inner])
      :- LogicalProject(AptYm=[AS($0, _UTF-16LE'AptYm')], sr_return_time_sk_node_4=[$1], sr_item_sk_node_4=[$2], sr_customer_sk_node_4=[$3], sr_cdemo_sk_node_4=[$4], sr_hdemo_sk_node_4=[$5], sr_addr_sk_node_4=[$6], sr_store_sk_node_4=[$7], sr_reason_sk_node_4=[$8], sr_ticket_number_node_4=[$9], sr_return_quantity_node_4=[$10], sr_return_amt_node_4=[$11], sr_return_tax_node_4=[$12], sr_return_amt_inc_tax_node_4=[$13], sr_fee_node_4=[$14], sr_return_ship_cost_node_4=[$15], sr_refunded_cash_node_4=[$16], sr_reversed_charge_node_4=[$17], sr_store_credit_node_4=[$18], sr_net_loss_node_4=[$19])
      :  +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
      +- LogicalSort(sort0=[$6], dir0=[ASC])
         +- LogicalProject(ca_address_sk_node_5=[$0], ca_address_id_node_5=[$1], ca_street_number_node_5=[$2], ca_street_name_node_5=[$3], ca_street_type_node_5=[$4], ca_suite_number_node_5=[$5], ca_city_node_5=[$6], ca_county_node_5=[$7], ca_state_node_5=[$8], ca_zip_node_5=[$9], ca_country_node_5=[$10], ca_gmt_offset_node_5=[$11], ca_location_type_node_5=[$12])
            +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS sr_store_credit_node_4])
+- HashAggregate(isMerge=[false], groupBy=[ca_county_node_5], select=[ca_county_node_5, MIN(sr_store_credit_node_4) AS EXPR$0])
   +- Exchange(distribution=[hash[ca_county_node_5]])
      +- Calc(select=[AptYm, sr_return_time_sk_node_4, sr_item_sk_node_4, sr_customer_sk_node_4, sr_cdemo_sk_node_4, sr_hdemo_sk_node_4, sr_addr_sk_node_4, sr_store_sk_node_4, sr_reason_sk_node_4, sr_ticket_number_node_4, sr_return_quantity_node_4, sr_return_amt_node_4, sr_return_tax_node_4, sr_return_amt_inc_tax_node_4, sr_fee_node_4, sr_return_ship_cost_node_4, sr_refunded_cash_node_4, sr_reversed_charge_node_4, sr_store_credit_node_4, sr_net_loss_node_4, ca_address_sk_node_5, ca_address_id_node_5, ca_street_number_node_5, ca_street_name_node_5, ca_street_type_node_5, ca_suite_number_node_5, ca_city_node_5, ca_county_node_5, ca_state_node_5, ca_zip_node_5, ca_country_node_5, ca_gmt_offset_node_5, ca_location_type_node_5])
         +- HashJoin(joinType=[InnerJoin], where=[=(sr_return_amt_node_4, ca_gmt_offset_node_50)], select=[AptYm, sr_return_time_sk_node_4, sr_item_sk_node_4, sr_customer_sk_node_4, sr_cdemo_sk_node_4, sr_hdemo_sk_node_4, sr_addr_sk_node_4, sr_store_sk_node_4, sr_reason_sk_node_4, sr_ticket_number_node_4, sr_return_quantity_node_4, sr_return_amt_node_4, sr_return_tax_node_4, sr_return_amt_inc_tax_node_4, sr_fee_node_4, sr_return_ship_cost_node_4, sr_refunded_cash_node_4, sr_reversed_charge_node_4, sr_store_credit_node_4, sr_net_loss_node_4, ca_address_sk_node_5, ca_address_id_node_5, ca_street_number_node_5, ca_street_name_node_5, ca_street_type_node_5, ca_suite_number_node_5, ca_city_node_5, ca_county_node_5, ca_state_node_5, ca_zip_node_5, ca_country_node_5, ca_gmt_offset_node_5, ca_location_type_node_5, ca_gmt_offset_node_50], build=[right])
            :- Exchange(distribution=[hash[sr_return_amt_node_4]])
            :  +- Calc(select=[sr_returned_date_sk AS AptYm, sr_return_time_sk AS sr_return_time_sk_node_4, sr_item_sk AS sr_item_sk_node_4, sr_customer_sk AS sr_customer_sk_node_4, sr_cdemo_sk AS sr_cdemo_sk_node_4, sr_hdemo_sk AS sr_hdemo_sk_node_4, sr_addr_sk AS sr_addr_sk_node_4, sr_store_sk AS sr_store_sk_node_4, sr_reason_sk AS sr_reason_sk_node_4, sr_ticket_number AS sr_ticket_number_node_4, sr_return_quantity AS sr_return_quantity_node_4, sr_return_amt AS sr_return_amt_node_4, sr_return_tax AS sr_return_tax_node_4, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_4, sr_fee AS sr_fee_node_4, sr_return_ship_cost AS sr_return_ship_cost_node_4, sr_refunded_cash AS sr_refunded_cash_node_4, sr_reversed_charge AS sr_reversed_charge_node_4, sr_store_credit AS sr_store_credit_node_4, sr_net_loss AS sr_net_loss_node_4])
            :     +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
            +- Exchange(distribution=[hash[ca_gmt_offset_node_50]])
               +- Calc(select=[ca_address_sk AS ca_address_sk_node_5, ca_address_id AS ca_address_id_node_5, ca_street_number AS ca_street_number_node_5, ca_street_name AS ca_street_name_node_5, ca_street_type AS ca_street_type_node_5, ca_suite_number AS ca_suite_number_node_5, ca_city AS ca_city_node_5, ca_county AS ca_county_node_5, ca_state AS ca_state_node_5, ca_zip AS ca_zip_node_5, ca_country AS ca_country_node_5, ca_gmt_offset AS ca_gmt_offset_node_5, ca_location_type AS ca_location_type_node_5, CAST(ca_gmt_offset AS DECIMAL(7, 2)) AS ca_gmt_offset_node_50])
                  +- Sort(orderBy=[ca_city ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS sr_store_credit_node_4])
+- HashAggregate(isMerge=[false], groupBy=[ca_county_node_5], select=[ca_county_node_5, MIN(sr_store_credit_node_4) AS EXPR$0])
   +- Exchange(distribution=[hash[ca_county_node_5]])
      +- Calc(select=[AptYm, sr_return_time_sk_node_4, sr_item_sk_node_4, sr_customer_sk_node_4, sr_cdemo_sk_node_4, sr_hdemo_sk_node_4, sr_addr_sk_node_4, sr_store_sk_node_4, sr_reason_sk_node_4, sr_ticket_number_node_4, sr_return_quantity_node_4, sr_return_amt_node_4, sr_return_tax_node_4, sr_return_amt_inc_tax_node_4, sr_fee_node_4, sr_return_ship_cost_node_4, sr_refunded_cash_node_4, sr_reversed_charge_node_4, sr_store_credit_node_4, sr_net_loss_node_4, ca_address_sk_node_5, ca_address_id_node_5, ca_street_number_node_5, ca_street_name_node_5, ca_street_type_node_5, ca_suite_number_node_5, ca_city_node_5, ca_county_node_5, ca_state_node_5, ca_zip_node_5, ca_country_node_5, ca_gmt_offset_node_5, ca_location_type_node_5])
         +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(sr_return_amt_node_4 = ca_gmt_offset_node_50)], select=[AptYm, sr_return_time_sk_node_4, sr_item_sk_node_4, sr_customer_sk_node_4, sr_cdemo_sk_node_4, sr_hdemo_sk_node_4, sr_addr_sk_node_4, sr_store_sk_node_4, sr_reason_sk_node_4, sr_ticket_number_node_4, sr_return_quantity_node_4, sr_return_amt_node_4, sr_return_tax_node_4, sr_return_amt_inc_tax_node_4, sr_fee_node_4, sr_return_ship_cost_node_4, sr_refunded_cash_node_4, sr_reversed_charge_node_4, sr_store_credit_node_4, sr_net_loss_node_4, ca_address_sk_node_5, ca_address_id_node_5, ca_street_number_node_5, ca_street_name_node_5, ca_street_type_node_5, ca_suite_number_node_5, ca_city_node_5, ca_county_node_5, ca_state_node_5, ca_zip_node_5, ca_country_node_5, ca_gmt_offset_node_5, ca_location_type_node_5, ca_gmt_offset_node_50], build=[right])
            :- Exchange(distribution=[hash[sr_return_amt_node_4]])
            :  +- Calc(select=[sr_returned_date_sk AS AptYm, sr_return_time_sk AS sr_return_time_sk_node_4, sr_item_sk AS sr_item_sk_node_4, sr_customer_sk AS sr_customer_sk_node_4, sr_cdemo_sk AS sr_cdemo_sk_node_4, sr_hdemo_sk AS sr_hdemo_sk_node_4, sr_addr_sk AS sr_addr_sk_node_4, sr_store_sk AS sr_store_sk_node_4, sr_reason_sk AS sr_reason_sk_node_4, sr_ticket_number AS sr_ticket_number_node_4, sr_return_quantity AS sr_return_quantity_node_4, sr_return_amt AS sr_return_amt_node_4, sr_return_tax AS sr_return_tax_node_4, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_4, sr_fee AS sr_fee_node_4, sr_return_ship_cost AS sr_return_ship_cost_node_4, sr_refunded_cash AS sr_refunded_cash_node_4, sr_reversed_charge AS sr_reversed_charge_node_4, sr_store_credit AS sr_store_credit_node_4, sr_net_loss AS sr_net_loss_node_4])
            :     +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
            +- Exchange(distribution=[hash[ca_gmt_offset_node_50]])
               +- Calc(select=[ca_address_sk AS ca_address_sk_node_5, ca_address_id AS ca_address_id_node_5, ca_street_number AS ca_street_number_node_5, ca_street_name AS ca_street_name_node_5, ca_street_type AS ca_street_type_node_5, ca_suite_number AS ca_suite_number_node_5, ca_city AS ca_city_node_5, ca_county AS ca_county_node_5, ca_state AS ca_state_node_5, ca_zip AS ca_zip_node_5, ca_country AS ca_country_node_5, ca_gmt_offset AS ca_gmt_offset_node_5, ca_location_type AS ca_location_type_node_5, CAST(ca_gmt_offset AS DECIMAL(7, 2)) AS ca_gmt_offset_node_50])
                  +- Sort(orderBy=[ca_city ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o112815108.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#227621578:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[6](input=RelSubset#227621576,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[6]), rel#227621575:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[6](input=RelSubset#227621574,groupBy=ca_county_node_5, ca_gmt_offset_node_50,select=ca_county_node_5, ca_gmt_offset_node_50)]
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