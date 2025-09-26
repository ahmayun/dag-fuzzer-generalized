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
    return values.count()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_12 = table_env.from_path("reason").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("reason").get_schema().get_field_names()])
autonode_14 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_10 = autonode_13.add_columns(lit("hello"))
autonode_9 = autonode_12.filter(col('r_reason_desc_node_12').char_length < 5)
autonode_11 = autonode_14.distinct()
autonode_7 = autonode_9.join(autonode_10, col('w_warehouse_sk_node_13') == col('r_reason_sk_node_12'))
autonode_8 = autonode_11.order_by(col('ca_state_node_14'))
autonode_5 = autonode_7.limit(22)
autonode_6 = autonode_8.filter(col('ca_location_type_node_14').char_length < 5)
autonode_3 = autonode_5.order_by(col('w_warehouse_id_node_13'))
autonode_4 = autonode_6.alias('VO8Dj')
autonode_2 = autonode_3.join(autonode_4, col('w_gmt_offset_node_13') == col('ca_gmt_offset_node_14'))
autonode_1 = autonode_2.group_by(col('w_street_number_node_13')).select(col('ca_street_type_node_14').max.alias('ca_street_type_node_14'))
sink = autonode_1.select(col('ca_street_type_node_14'))
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
      "error_message": "An error occurred while calling o113923840.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#229870939:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[4](input=RelSubset#229870937,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[4]), rel#229870936:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[4](input=RelSubset#229870935,groupBy=w_street_number_node_13, w_gmt_offset_node_13,select=w_street_number_node_13, w_gmt_offset_node_13)]
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
    },
    "unopt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalProject(ca_street_type_node_14=[$1])
+- LogicalAggregate(group=[{7}], EXPR$0=[MAX($22)])
   +- LogicalJoin(condition=[=($16, $29)], joinType=[inner])
      :- LogicalSort(sort0=[$4], dir0=[ASC])
      :  +- LogicalSort(fetch=[22])
      :     +- LogicalJoin(condition=[=($3, $0)], joinType=[inner])
      :        :- LogicalFilter(condition=[<(CHAR_LENGTH($2), 5)])
      :        :  +- LogicalProject(r_reason_sk_node_12=[$0], r_reason_id_node_12=[$1], r_reason_desc_node_12=[$2])
      :        :     +- LogicalTableScan(table=[[default_catalog, default_database, reason]])
      :        +- LogicalProject(w_warehouse_sk_node_13=[$0], w_warehouse_id_node_13=[$1], w_warehouse_name_node_13=[$2], w_warehouse_sq_ft_node_13=[$3], w_street_number_node_13=[$4], w_street_name_node_13=[$5], w_street_type_node_13=[$6], w_suite_number_node_13=[$7], w_city_node_13=[$8], w_county_node_13=[$9], w_state_node_13=[$10], w_zip_node_13=[$11], w_country_node_13=[$12], w_gmt_offset_node_13=[$13], _c14=[_UTF-16LE'hello'])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])
      +- LogicalProject(VO8Dj=[AS($0, _UTF-16LE'VO8Dj')], ca_address_id_node_14=[$1], ca_street_number_node_14=[$2], ca_street_name_node_14=[$3], ca_street_type_node_14=[$4], ca_suite_number_node_14=[$5], ca_city_node_14=[$6], ca_county_node_14=[$7], ca_state_node_14=[$8], ca_zip_node_14=[$9], ca_country_node_14=[$10], ca_gmt_offset_node_14=[$11], ca_location_type_node_14=[$12])
         +- LogicalFilter(condition=[<(CHAR_LENGTH($12), 5)])
            +- LogicalSort(sort0=[$8], dir0=[ASC])
               +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}])
                  +- LogicalProject(ca_address_sk_node_14=[$0], ca_address_id_node_14=[$1], ca_street_number_node_14=[$2], ca_street_name_node_14=[$3], ca_street_type_node_14=[$4], ca_suite_number_node_14=[$5], ca_city_node_14=[$6], ca_county_node_14=[$7], ca_state_node_14=[$8], ca_zip_node_14=[$9], ca_country_node_14=[$10], ca_gmt_offset_node_14=[$11], ca_location_type_node_14=[$12])
                     +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ca_street_type_node_14])
+- SortAggregate(isMerge=[false], groupBy=[w_street_number_node_13], select=[w_street_number_node_13, MAX(ca_street_type_node_14) AS EXPR$0])
   +- Sort(orderBy=[w_street_number_node_13 ASC])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(w_gmt_offset_node_13, ca_gmt_offset_node_14)], select=[r_reason_sk, r_reason_id, r_reason_desc, w_warehouse_sk_node_13, w_warehouse_id_node_13, w_warehouse_name_node_13, w_warehouse_sq_ft_node_13, w_street_number_node_13, w_street_name_node_13, w_street_type_node_13, w_suite_number_node_13, w_city_node_13, w_county_node_13, w_state_node_13, w_zip_node_13, w_country_node_13, w_gmt_offset_node_13, _c14, VO8Dj, ca_address_id_node_14, ca_street_number_node_14, ca_street_name_node_14, ca_street_type_node_14, ca_suite_number_node_14, ca_city_node_14, ca_county_node_14, ca_state_node_14, ca_zip_node_14, ca_country_node_14, ca_gmt_offset_node_14, ca_location_type_node_14], build=[right])
         :- Exchange(distribution=[hash[w_street_number_node_13]])
         :  +- SortLimit(orderBy=[w_warehouse_id_node_13 ASC], offset=[0], fetch=[1], global=[true])
         :     +- Exchange(distribution=[single])
         :        +- SortLimit(orderBy=[w_warehouse_id_node_13 ASC], offset=[0], fetch=[1], global=[false])
         :           +- Limit(offset=[0], fetch=[22], global=[true])
         :              +- Exchange(distribution=[single])
         :                 +- Limit(offset=[0], fetch=[22], global=[false])
         :                    +- NestedLoopJoin(joinType=[InnerJoin], where=[=(w_warehouse_sk_node_13, r_reason_sk)], select=[r_reason_sk, r_reason_id, r_reason_desc, w_warehouse_sk_node_13, w_warehouse_id_node_13, w_warehouse_name_node_13, w_warehouse_sq_ft_node_13, w_street_number_node_13, w_street_name_node_13, w_street_type_node_13, w_suite_number_node_13, w_city_node_13, w_county_node_13, w_state_node_13, w_zip_node_13, w_country_node_13, w_gmt_offset_node_13, _c14], build=[left])
         :                       :- Exchange(distribution=[broadcast])
         :                       :  +- Calc(select=[r_reason_sk, r_reason_id, r_reason_desc], where=[<(CHAR_LENGTH(r_reason_desc), 5)])
         :                       :     +- TableSourceScan(table=[[default_catalog, default_database, reason, filter=[<(CHAR_LENGTH(r_reason_desc), 5)]]], fields=[r_reason_sk, r_reason_id, r_reason_desc])
         :                       +- Calc(select=[w_warehouse_sk AS w_warehouse_sk_node_13, w_warehouse_id AS w_warehouse_id_node_13, w_warehouse_name AS w_warehouse_name_node_13, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_13, w_street_number AS w_street_number_node_13, w_street_name AS w_street_name_node_13, w_street_type AS w_street_type_node_13, w_suite_number AS w_suite_number_node_13, w_city AS w_city_node_13, w_county AS w_county_node_13, w_state AS w_state_node_13, w_zip AS w_zip_node_13, w_country AS w_country_node_13, w_gmt_offset AS w_gmt_offset_node_13, 'hello' AS _c14])
         :                          +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[ca_address_sk AS VO8Dj, ca_address_id AS ca_address_id_node_14, ca_street_number AS ca_street_number_node_14, ca_street_name AS ca_street_name_node_14, ca_street_type AS ca_street_type_node_14, ca_suite_number AS ca_suite_number_node_14, ca_city AS ca_city_node_14, ca_county AS ca_county_node_14, ca_state AS ca_state_node_14, ca_zip AS ca_zip_node_14, ca_country AS ca_country_node_14, ca_gmt_offset AS ca_gmt_offset_node_14, ca_location_type AS ca_location_type_node_14], where=[<(CHAR_LENGTH(ca_location_type), 5)])
               +- SortLimit(orderBy=[ca_state ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[ca_state ASC], offset=[0], fetch=[1], global=[false])
                        +- HashAggregate(isMerge=[false], groupBy=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                           +- Exchange(distribution=[hash[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type]])
                              +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ca_street_type_node_14])
+- SortAggregate(isMerge=[false], groupBy=[w_street_number_node_13], select=[w_street_number_node_13, MAX(ca_street_type_node_14) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[w_street_number_node_13 ASC])
         +- Exchange(distribution=[keep_input_as_is[hash[w_street_number_node_13]]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(w_gmt_offset_node_13 = ca_gmt_offset_node_14)], select=[r_reason_sk, r_reason_id, r_reason_desc, w_warehouse_sk_node_13, w_warehouse_id_node_13, w_warehouse_name_node_13, w_warehouse_sq_ft_node_13, w_street_number_node_13, w_street_name_node_13, w_street_type_node_13, w_suite_number_node_13, w_city_node_13, w_county_node_13, w_state_node_13, w_zip_node_13, w_country_node_13, w_gmt_offset_node_13, _c14, VO8Dj, ca_address_id_node_14, ca_street_number_node_14, ca_street_name_node_14, ca_street_type_node_14, ca_suite_number_node_14, ca_city_node_14, ca_county_node_14, ca_state_node_14, ca_zip_node_14, ca_country_node_14, ca_gmt_offset_node_14, ca_location_type_node_14], build=[right])
               :- Exchange(distribution=[hash[w_street_number_node_13]])
               :  +- SortLimit(orderBy=[w_warehouse_id_node_13 ASC], offset=[0], fetch=[1], global=[true])
               :     +- Exchange(distribution=[single])
               :        +- SortLimit(orderBy=[w_warehouse_id_node_13 ASC], offset=[0], fetch=[1], global=[false])
               :           +- Limit(offset=[0], fetch=[22], global=[true])
               :              +- Exchange(distribution=[single])
               :                 +- Limit(offset=[0], fetch=[22], global=[false])
               :                    +- NestedLoopJoin(joinType=[InnerJoin], where=[(w_warehouse_sk_node_13 = r_reason_sk)], select=[r_reason_sk, r_reason_id, r_reason_desc, w_warehouse_sk_node_13, w_warehouse_id_node_13, w_warehouse_name_node_13, w_warehouse_sq_ft_node_13, w_street_number_node_13, w_street_name_node_13, w_street_type_node_13, w_suite_number_node_13, w_city_node_13, w_county_node_13, w_state_node_13, w_zip_node_13, w_country_node_13, w_gmt_offset_node_13, _c14], build=[left])
               :                       :- Exchange(distribution=[broadcast])
               :                       :  +- Calc(select=[r_reason_sk, r_reason_id, r_reason_desc], where=[(CHAR_LENGTH(r_reason_desc) < 5)])
               :                       :     +- TableSourceScan(table=[[default_catalog, default_database, reason, filter=[<(CHAR_LENGTH(r_reason_desc), 5)]]], fields=[r_reason_sk, r_reason_id, r_reason_desc])
               :                       +- Calc(select=[w_warehouse_sk AS w_warehouse_sk_node_13, w_warehouse_id AS w_warehouse_id_node_13, w_warehouse_name AS w_warehouse_name_node_13, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_13, w_street_number AS w_street_number_node_13, w_street_name AS w_street_name_node_13, w_street_type AS w_street_type_node_13, w_suite_number AS w_suite_number_node_13, w_city AS w_city_node_13, w_county AS w_county_node_13, w_state AS w_state_node_13, w_zip AS w_zip_node_13, w_country AS w_country_node_13, w_gmt_offset AS w_gmt_offset_node_13, 'hello' AS _c14])
               :                          +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
               +- Exchange(distribution=[broadcast])
                  +- Calc(select=[ca_address_sk AS VO8Dj, ca_address_id AS ca_address_id_node_14, ca_street_number AS ca_street_number_node_14, ca_street_name AS ca_street_name_node_14, ca_street_type AS ca_street_type_node_14, ca_suite_number AS ca_suite_number_node_14, ca_city AS ca_city_node_14, ca_county AS ca_county_node_14, ca_state AS ca_state_node_14, ca_zip AS ca_zip_node_14, ca_country AS ca_country_node_14, ca_gmt_offset AS ca_gmt_offset_node_14, ca_location_type AS ca_location_type_node_14], where=[(CHAR_LENGTH(ca_location_type) < 5)])
                     +- SortLimit(orderBy=[ca_state ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[ca_state ASC], offset=[0], fetch=[1], global=[false])
                              +- HashAggregate(isMerge=[false], groupBy=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                                 +- Exchange(distribution=[hash[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type]])
                                    +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0