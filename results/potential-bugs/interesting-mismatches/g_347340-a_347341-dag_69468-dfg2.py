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

autonode_6 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_7 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_4 = autonode_6.order_by(col('ca_street_number_node_6'))
autonode_5 = autonode_7.limit(52)
autonode_3 = autonode_4.join(autonode_5, col('ca_suite_number_node_6') == col('hd_buy_potential_node_7'))
autonode_2 = autonode_3.group_by(col('hd_buy_potential_node_7')).select(col('ca_gmt_offset_node_6').min.alias('ca_gmt_offset_node_6'))
autonode_1 = autonode_2.order_by(col('ca_gmt_offset_node_6'))
sink = autonode_1.distinct()
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
LogicalAggregate(group=[{0}])
+- LogicalSort(sort0=[$0], dir0=[ASC])
   +- LogicalProject(ca_gmt_offset_node_6=[$1])
      +- LogicalAggregate(group=[{15}], EXPR$0=[MIN($11)])
         +- LogicalJoin(condition=[=($5, $15)], joinType=[inner])
            :- LogicalSort(sort0=[$2], dir0=[ASC])
            :  +- LogicalProject(ca_address_sk_node_6=[$0], ca_address_id_node_6=[$1], ca_street_number_node_6=[$2], ca_street_name_node_6=[$3], ca_street_type_node_6=[$4], ca_suite_number_node_6=[$5], ca_city_node_6=[$6], ca_county_node_6=[$7], ca_state_node_6=[$8], ca_zip_node_6=[$9], ca_country_node_6=[$10], ca_gmt_offset_node_6=[$11], ca_location_type_node_6=[$12])
            :     +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
            +- LogicalSort(fetch=[52])
               +- LogicalProject(hd_demo_sk_node_7=[$0], hd_income_band_sk_node_7=[$1], hd_buy_potential_node_7=[$2], hd_dep_count_node_7=[$3], hd_vehicle_count_node_7=[$4])
                  +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])

== Optimized Physical Plan ==
HashAggregate(isMerge=[true], groupBy=[ca_gmt_offset_node_6], select=[ca_gmt_offset_node_6])
+- Sort(orderBy=[ca_gmt_offset_node_6 ASC])
   +- Exchange(distribution=[hash[ca_gmt_offset_node_6]])
      +- LocalHashAggregate(groupBy=[ca_gmt_offset_node_6], select=[ca_gmt_offset_node_6])
         +- Sort(orderBy=[ca_gmt_offset_node_6 ASC])
            +- Exchange(distribution=[single])
               +- Calc(select=[EXPR$0 AS ca_gmt_offset_node_6])
                  +- HashAggregate(isMerge=[true], groupBy=[hd_buy_potential], select=[hd_buy_potential, Final_MIN(min$0) AS EXPR$0])
                     +- Exchange(distribution=[hash[hd_buy_potential]])
                        +- LocalHashAggregate(groupBy=[hd_buy_potential], select=[hd_buy_potential, Partial_MIN(ca_gmt_offset) AS min$0])
                           +- HashJoin(joinType=[InnerJoin], where=[=(ca_suite_number, hd_buy_potential)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], isBroadcast=[true], build=[right])
                              :- Sort(orderBy=[ca_street_number ASC])
                              :  +- Exchange(distribution=[single])
                              :     +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                              +- Exchange(distribution=[broadcast])
                                 +- Limit(offset=[0], fetch=[52], global=[true])
                                    +- Exchange(distribution=[single])
                                       +- Limit(offset=[0], fetch=[52], global=[false])
                                          +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, limit=[52]]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])

== Optimized Execution Plan ==
HashAggregate(isMerge=[true], groupBy=[ca_gmt_offset_node_6], select=[ca_gmt_offset_node_6])
+- Exchange(distribution=[keep_input_as_is[hash[ca_gmt_offset_node_6]]])
   +- Sort(orderBy=[ca_gmt_offset_node_6 ASC])
      +- Exchange(distribution=[hash[ca_gmt_offset_node_6]])
         +- LocalHashAggregate(groupBy=[ca_gmt_offset_node_6], select=[ca_gmt_offset_node_6])
            +- Sort(orderBy=[ca_gmt_offset_node_6 ASC])
               +- Exchange(distribution=[single])
                  +- Calc(select=[EXPR$0 AS ca_gmt_offset_node_6])
                     +- HashAggregate(isMerge=[true], groupBy=[hd_buy_potential], select=[hd_buy_potential, Final_MIN(min$0) AS EXPR$0])
                        +- Exchange(distribution=[hash[hd_buy_potential]])
                           +- LocalHashAggregate(groupBy=[hd_buy_potential], select=[hd_buy_potential, Partial_MIN(ca_gmt_offset) AS min$0])
                              +- HashJoin(joinType=[InnerJoin], where=[(ca_suite_number = hd_buy_potential)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], isBroadcast=[true], build=[right])
                                 :- Sort(orderBy=[ca_street_number ASC])
                                 :  +- Exchange(distribution=[single])
                                 :     +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                                 +- Exchange(distribution=[broadcast])
                                    +- Limit(offset=[0], fetch=[52], global=[true])
                                       +- Exchange(distribution=[single])
                                          +- Limit(offset=[0], fetch=[52], global=[false])
                                             +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, limit=[52]]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o189030932.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#382409291:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[2](input=RelSubset#382409289,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[2]), rel#382409288:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[2](input=RelSubset#382409287,groupBy=ca_suite_number,select=ca_suite_number, Partial_MIN(ca_gmt_offset) AS min$0)]
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