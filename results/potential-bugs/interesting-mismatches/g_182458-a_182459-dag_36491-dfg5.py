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
    return values.nunique()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_5 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_7 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_3 = autonode_5.join(autonode_6, col('web_gmt_offset_node_5') == col('ca_gmt_offset_node_6'))
autonode_4 = autonode_7.order_by(col('cd_marital_status_node_7'))
autonode_2 = autonode_3.join(autonode_4, col('cd_demo_sk_node_7') == col('ca_address_sk_node_6'))
autonode_1 = autonode_2.group_by(col('web_street_name_node_5')).select(col('web_site_sk_node_5').min.alias('web_site_sk_node_5'))
sink = autonode_1.filter(col('web_site_sk_node_5') > -4)
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
      "error_message": "An error occurred while calling o99483925.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#200609578:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[2](input=RelSubset#200609576,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[2]), rel#200609575:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[2](input=RelSubset#200609574,groupBy=cd_demo_sk,select=cd_demo_sk)]
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
LogicalFilter(condition=[>($0, -4)])
+- LogicalProject(web_site_sk_node_5=[$1])
   +- LogicalAggregate(group=[{16}], EXPR$0=[MIN($0)])
      +- LogicalJoin(condition=[=($39, $26)], joinType=[inner])
         :- LogicalJoin(condition=[=($24, $37)], joinType=[inner])
         :  :- LogicalProject(web_site_sk_node_5=[$0], web_site_id_node_5=[$1], web_rec_start_date_node_5=[$2], web_rec_end_date_node_5=[$3], web_name_node_5=[$4], web_open_date_sk_node_5=[$5], web_close_date_sk_node_5=[$6], web_class_node_5=[$7], web_manager_node_5=[$8], web_mkt_id_node_5=[$9], web_mkt_class_node_5=[$10], web_mkt_desc_node_5=[$11], web_market_manager_node_5=[$12], web_company_id_node_5=[$13], web_company_name_node_5=[$14], web_street_number_node_5=[$15], web_street_name_node_5=[$16], web_street_type_node_5=[$17], web_suite_number_node_5=[$18], web_city_node_5=[$19], web_county_node_5=[$20], web_state_node_5=[$21], web_zip_node_5=[$22], web_country_node_5=[$23], web_gmt_offset_node_5=[$24], web_tax_percentage_node_5=[$25])
         :  :  +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])
         :  +- LogicalProject(ca_address_sk_node_6=[$0], ca_address_id_node_6=[$1], ca_street_number_node_6=[$2], ca_street_name_node_6=[$3], ca_street_type_node_6=[$4], ca_suite_number_node_6=[$5], ca_city_node_6=[$6], ca_county_node_6=[$7], ca_state_node_6=[$8], ca_zip_node_6=[$9], ca_country_node_6=[$10], ca_gmt_offset_node_6=[$11], ca_location_type_node_6=[$12])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
         +- LogicalSort(sort0=[$2], dir0=[ASC])
            +- LogicalProject(cd_demo_sk_node_7=[$0], cd_gender_node_7=[$1], cd_marital_status_node_7=[$2], cd_education_status_node_7=[$3], cd_purchase_estimate_node_7=[$4], cd_credit_rating_node_7=[$5], cd_dep_count_node_7=[$6], cd_dep_employed_count_node_7=[$7], cd_dep_college_count_node_7=[$8])
               +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0], where=[>(EXPR$0, -4)])
+- HashAggregate(isMerge=[true], groupBy=[web_street_name], select=[web_street_name, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[web_street_name]])
      +- LocalHashAggregate(groupBy=[web_street_name], select=[web_street_name, Partial_MIN(web_site_sk) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cd_demo_sk, ca_address_sk)], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[right])
            :- HashJoin(joinType=[InnerJoin], where=[=(web_gmt_offset, ca_gmt_offset)], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], isBroadcast=[true], build=[left])
            :  :- Exchange(distribution=[broadcast])
            :  :  +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
            :  +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
            +- Exchange(distribution=[broadcast])
               +- SortLimit(orderBy=[cd_marital_status ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[cd_marital_status ASC], offset=[0], fetch=[1], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0], where=[(EXPR$0 > -4)])
+- HashAggregate(isMerge=[true], groupBy=[web_street_name], select=[web_street_name, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[web_street_name]])
      +- LocalHashAggregate(groupBy=[web_street_name], select=[web_street_name, Partial_MIN(web_site_sk) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(cd_demo_sk = ca_address_sk)], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[right])
            :- HashJoin(joinType=[InnerJoin], where=[(web_gmt_offset = ca_gmt_offset)], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], isBroadcast=[true], build=[left])
            :  :- Exchange(distribution=[broadcast])
            :  :  +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
            :  +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
            +- Exchange(distribution=[broadcast])
               +- SortLimit(orderBy=[cd_marital_status ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[cd_marital_status ASC], offset=[0], fetch=[1], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0