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
    return len(values) / (1.0 / values).sum() if (values != 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_4 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_4") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_5 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_2 = autonode_4.order_by(col('hd_income_band_sk_node_4'))
autonode_3 = autonode_5.alias('bGa2I')
autonode_1 = autonode_2.join(autonode_3, col('hd_buy_potential_node_4') == col('wp_rec_start_date_node_5'))
sink = autonode_1.group_by(col('wp_web_page_id_node_5')).select(col('wp_image_count_node_5').max.alias('wp_image_count_node_5'))
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
      "error_message": "An error occurred while calling o170988457.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#345614532:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[1](input=RelSubset#345614530,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[1]), rel#345614529:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[1](input=RelSubset#345614528,groupBy=hd_buy_potential,select=hd_buy_potential)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (1) must be less than size (1)
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
LogicalProject(wp_image_count_node_5=[$1])
+- LogicalAggregate(group=[{6}], EXPR$0=[MAX($17)])
   +- LogicalJoin(condition=[=($2, $7)], joinType=[inner])
      :- LogicalSort(sort0=[$1], dir0=[ASC])
      :  +- LogicalProject(hd_demo_sk_node_4=[$0], hd_income_band_sk_node_4=[$1], hd_buy_potential_node_4=[$2], hd_dep_count_node_4=[$3], hd_vehicle_count_node_4=[$4])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
      +- LogicalProject(bGa2I=[AS($0, _UTF-16LE'bGa2I')], wp_web_page_id_node_5=[$1], wp_rec_start_date_node_5=[$2], wp_rec_end_date_node_5=[$3], wp_creation_date_sk_node_5=[$4], wp_access_date_sk_node_5=[$5], wp_autogen_flag_node_5=[$6], wp_customer_sk_node_5=[$7], wp_url_node_5=[$8], wp_type_node_5=[$9], wp_char_count_node_5=[$10], wp_link_count_node_5=[$11], wp_image_count_node_5=[$12], wp_max_ad_count_node_5=[$13])
         +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS wp_image_count_node_5])
+- HashAggregate(isMerge=[true], groupBy=[wp_web_page_id_node_5], select=[wp_web_page_id_node_5, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[wp_web_page_id_node_5]])
      +- LocalHashAggregate(groupBy=[wp_web_page_id_node_5], select=[wp_web_page_id_node_5, Partial_MAX(wp_image_count_node_5) AS max$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(hd_buy_potential, wp_rec_start_date_node_5)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, bGa2I, wp_web_page_id_node_5, wp_rec_start_date_node_5, wp_rec_end_date_node_5, wp_creation_date_sk_node_5, wp_access_date_sk_node_5, wp_autogen_flag_node_5, wp_customer_sk_node_5, wp_url_node_5, wp_type_node_5, wp_char_count_node_5, wp_link_count_node_5, wp_image_count_node_5, wp_max_ad_count_node_5], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- SortLimit(orderBy=[hd_income_band_sk ASC], offset=[0], fetch=[1], global=[true])
            :     +- Exchange(distribution=[single])
            :        +- SortLimit(orderBy=[hd_income_band_sk ASC], offset=[0], fetch=[1], global=[false])
            :           +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
            +- Calc(select=[wp_web_page_sk AS bGa2I, wp_web_page_id AS wp_web_page_id_node_5, wp_rec_start_date AS wp_rec_start_date_node_5, wp_rec_end_date AS wp_rec_end_date_node_5, wp_creation_date_sk AS wp_creation_date_sk_node_5, wp_access_date_sk AS wp_access_date_sk_node_5, wp_autogen_flag AS wp_autogen_flag_node_5, wp_customer_sk AS wp_customer_sk_node_5, wp_url AS wp_url_node_5, wp_type AS wp_type_node_5, wp_char_count AS wp_char_count_node_5, wp_link_count AS wp_link_count_node_5, wp_image_count AS wp_image_count_node_5, wp_max_ad_count AS wp_max_ad_count_node_5])
               +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS wp_image_count_node_5])
+- HashAggregate(isMerge=[true], groupBy=[wp_web_page_id_node_5], select=[wp_web_page_id_node_5, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[wp_web_page_id_node_5]])
      +- LocalHashAggregate(groupBy=[wp_web_page_id_node_5], select=[wp_web_page_id_node_5, Partial_MAX(wp_image_count_node_5) AS max$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(hd_buy_potential = wp_rec_start_date_node_5)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, bGa2I, wp_web_page_id_node_5, wp_rec_start_date_node_5, wp_rec_end_date_node_5, wp_creation_date_sk_node_5, wp_access_date_sk_node_5, wp_autogen_flag_node_5, wp_customer_sk_node_5, wp_url_node_5, wp_type_node_5, wp_char_count_node_5, wp_link_count_node_5, wp_image_count_node_5, wp_max_ad_count_node_5], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- SortLimit(orderBy=[hd_income_band_sk ASC], offset=[0], fetch=[1], global=[true])
            :     +- Exchange(distribution=[single])
            :        +- SortLimit(orderBy=[hd_income_band_sk ASC], offset=[0], fetch=[1], global=[false])
            :           +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
            +- Calc(select=[wp_web_page_sk AS bGa2I, wp_web_page_id AS wp_web_page_id_node_5, wp_rec_start_date AS wp_rec_start_date_node_5, wp_rec_end_date AS wp_rec_end_date_node_5, wp_creation_date_sk AS wp_creation_date_sk_node_5, wp_access_date_sk AS wp_access_date_sk_node_5, wp_autogen_flag AS wp_autogen_flag_node_5, wp_customer_sk AS wp_customer_sk_node_5, wp_url AS wp_url_node_5, wp_type AS wp_type_node_5, wp_char_count AS wp_char_count_node_5, wp_link_count AS wp_link_count_node_5, wp_image_count AS wp_image_count_node_5, wp_max_ad_count AS wp_max_ad_count_node_5])
               +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0