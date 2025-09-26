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
    return values.median()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_13 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_12 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_11 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_9 = autonode_13.filter(col('cp_catalog_page_number_node_13') < -46)
autonode_8 = autonode_12.order_by(col('hd_income_band_sk_node_12'))
autonode_7 = autonode_10.join(autonode_11, col('web_street_type_node_10') == col('cd_education_status_node_11'))
autonode_6 = autonode_8.join(autonode_9, col('cp_end_date_sk_node_13') == col('hd_income_band_sk_node_12'))
autonode_5 = autonode_7.order_by(col('web_company_id_node_10'))
autonode_4 = autonode_5.join(autonode_6, col('web_mkt_id_node_10') == col('hd_demo_sk_node_12'))
autonode_3 = autonode_4.group_by(col('cp_catalog_page_sk_node_13')).select(col('cp_start_date_sk_node_13').max.alias('cp_start_date_sk_node_13'))
autonode_2 = autonode_3.add_columns(lit("hello"))
autonode_1 = autonode_2.select(col('cp_start_date_sk_node_13'))
sink = autonode_1.group_by(col('cp_start_date_sk_node_13')).select(col('cp_start_date_sk_node_13').count.alias('cp_start_date_sk_node_13'))
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
      "error_message": "An error occurred while calling o299828599.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#604650540:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[13](input=RelSubset#604650538,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[13]), rel#604650537:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[13](input=RelSubset#604650536,groupBy=web_mkt_id,select=web_mkt_id)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (13) must be less than size (1)
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
LogicalProject(cp_start_date_sk_node_13=[$1])
+- LogicalAggregate(group=[{0}], EXPR$0=[COUNT($0)])
   +- LogicalProject(cp_start_date_sk_node_13=[$1])
      +- LogicalAggregate(group=[{40}], EXPR$0=[MAX($42)])
         +- LogicalJoin(condition=[=($9, $35)], joinType=[inner])
            :- LogicalSort(sort0=[$13], dir0=[ASC])
            :  +- LogicalJoin(condition=[=($17, $29)], joinType=[inner])
            :     :- LogicalProject(web_site_sk_node_10=[$0], web_site_id_node_10=[$1], web_rec_start_date_node_10=[$2], web_rec_end_date_node_10=[$3], web_name_node_10=[$4], web_open_date_sk_node_10=[$5], web_close_date_sk_node_10=[$6], web_class_node_10=[$7], web_manager_node_10=[$8], web_mkt_id_node_10=[$9], web_mkt_class_node_10=[$10], web_mkt_desc_node_10=[$11], web_market_manager_node_10=[$12], web_company_id_node_10=[$13], web_company_name_node_10=[$14], web_street_number_node_10=[$15], web_street_name_node_10=[$16], web_street_type_node_10=[$17], web_suite_number_node_10=[$18], web_city_node_10=[$19], web_county_node_10=[$20], web_state_node_10=[$21], web_zip_node_10=[$22], web_country_node_10=[$23], web_gmt_offset_node_10=[$24], web_tax_percentage_node_10=[$25])
            :     :  +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])
            :     +- LogicalProject(cd_demo_sk_node_11=[$0], cd_gender_node_11=[$1], cd_marital_status_node_11=[$2], cd_education_status_node_11=[$3], cd_purchase_estimate_node_11=[$4], cd_credit_rating_node_11=[$5], cd_dep_count_node_11=[$6], cd_dep_employed_count_node_11=[$7], cd_dep_college_count_node_11=[$8])
            :        +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
            +- LogicalJoin(condition=[=($8, $1)], joinType=[inner])
               :- LogicalSort(sort0=[$1], dir0=[ASC])
               :  +- LogicalProject(hd_demo_sk_node_12=[$0], hd_income_band_sk_node_12=[$1], hd_buy_potential_node_12=[$2], hd_dep_count_node_12=[$3], hd_vehicle_count_node_12=[$4])
               :     +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
               +- LogicalFilter(condition=[<($6, -46)])
                  +- LogicalProject(cp_catalog_page_sk_node_13=[$0], cp_catalog_page_id_node_13=[$1], cp_start_date_sk_node_13=[$2], cp_end_date_sk_node_13=[$3], cp_department_node_13=[$4], cp_catalog_number_node_13=[$5], cp_catalog_page_number_node_13=[$6], cp_description_node_13=[$7], cp_type_node_13=[$8])
                     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cp_start_date_sk_node_13])
+- HashAggregate(isMerge=[true], groupBy=[cp_start_date_sk_node_13], select=[cp_start_date_sk_node_13, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cp_start_date_sk_node_13]])
      +- LocalHashAggregate(groupBy=[cp_start_date_sk_node_13], select=[cp_start_date_sk_node_13, Partial_COUNT(cp_start_date_sk_node_13) AS count$0])
         +- Calc(select=[EXPR$0 AS cp_start_date_sk_node_13])
            +- HashAggregate(isMerge=[false], groupBy=[cp_catalog_page_sk], select=[cp_catalog_page_sk, MAX(cp_start_date_sk) AS EXPR$0])
               +- Exchange(distribution=[hash[cp_catalog_page_sk]])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(web_mkt_id, hd_demo_sk)], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], build=[left])
                     :- Exchange(distribution=[broadcast])
                     :  +- SortLimit(orderBy=[web_company_id ASC], offset=[0], fetch=[1], global=[true])
                     :     +- Exchange(distribution=[single])
                     :        +- SortLimit(orderBy=[web_company_id ASC], offset=[0], fetch=[1], global=[false])
                     :           +- HashJoin(joinType=[InnerJoin], where=[=(web_street_type, cd_education_status)], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], isBroadcast=[true], build=[left])
                     :              :- Exchange(distribution=[broadcast])
                     :              :  +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                     :              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cp_end_date_sk, hd_income_band_sk)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- SortLimit(orderBy=[hd_income_band_sk ASC], offset=[0], fetch=[1], global=[true])
                        :     +- Exchange(distribution=[single])
                        :        +- SortLimit(orderBy=[hd_income_band_sk ASC], offset=[0], fetch=[1], global=[false])
                        :           +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                        +- Calc(select=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], where=[<(cp_catalog_page_number, -46)])
                           +- TableSourceScan(table=[[default_catalog, default_database, catalog_page, filter=[<(cp_catalog_page_number, -46)]]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cp_start_date_sk_node_13])
+- HashAggregate(isMerge=[true], groupBy=[cp_start_date_sk_node_13], select=[cp_start_date_sk_node_13, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cp_start_date_sk_node_13]])
      +- LocalHashAggregate(groupBy=[cp_start_date_sk_node_13], select=[cp_start_date_sk_node_13, Partial_COUNT(cp_start_date_sk_node_13) AS count$0])
         +- Calc(select=[EXPR$0 AS cp_start_date_sk_node_13])
            +- HashAggregate(isMerge=[false], groupBy=[cp_catalog_page_sk], select=[cp_catalog_page_sk, MAX(cp_start_date_sk) AS EXPR$0])
               +- Exchange(distribution=[hash[cp_catalog_page_sk]])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[(web_mkt_id = hd_demo_sk)], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], build=[left])
                     :- Exchange(distribution=[broadcast])
                     :  +- SortLimit(orderBy=[web_company_id ASC], offset=[0], fetch=[1], global=[true])
                     :     +- Exchange(distribution=[single])
                     :        +- SortLimit(orderBy=[web_company_id ASC], offset=[0], fetch=[1], global=[false])
                     :           +- HashJoin(joinType=[InnerJoin], where=[(web_street_type = cd_education_status)], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], isBroadcast=[true], build=[left])
                     :              :- Exchange(distribution=[broadcast])
                     :              :  +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                     :              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(cp_end_date_sk = hd_income_band_sk)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- SortLimit(orderBy=[hd_income_band_sk ASC], offset=[0], fetch=[1], global=[true])
                        :     +- Exchange(distribution=[single])
                        :        +- SortLimit(orderBy=[hd_income_band_sk ASC], offset=[0], fetch=[1], global=[false])
                        :           +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                        +- Calc(select=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], where=[(cp_catalog_page_number < -46)])
                           +- TableSourceScan(table=[[default_catalog, default_database, catalog_page, filter=[<(cp_catalog_page_number, -46)]]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0