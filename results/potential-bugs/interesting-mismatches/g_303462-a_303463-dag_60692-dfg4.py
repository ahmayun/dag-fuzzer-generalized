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
    return values.max()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_8 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_7 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_6 = autonode_8.distinct()
autonode_5 = autonode_7.order_by(col('c_current_cdemo_sk_node_7'))
autonode_4 = autonode_6.alias('uOYie')
autonode_3 = autonode_5.add_columns(lit("hello"))
autonode_2 = autonode_3.join(autonode_4, col('cc_mkt_id_node_8') == col('c_current_cdemo_sk_node_7'))
autonode_1 = autonode_2.group_by(col('cc_employees_node_8')).select(col('cc_employees_node_8').max.alias('cc_employees_node_8'))
sink = autonode_1.add_columns(lit("hello"))
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
      "error_message": "An error occurred while calling o165073089.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#333517785:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[2](input=RelSubset#333517783,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[2]), rel#333517782:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[2](input=RelSubset#333517781,groupBy=c_current_cdemo_sk,select=c_current_cdemo_sk)]
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
LogicalProject(cc_employees_node_8=[$1], _c1=[_UTF-16LE'hello'])
+- LogicalAggregate(group=[{27}], EXPR$0=[MAX($27)])
   +- LogicalJoin(condition=[=($31, $2)], joinType=[inner])
      :- LogicalProject(c_customer_sk_node_7=[$0], c_customer_id_node_7=[$1], c_current_cdemo_sk_node_7=[$2], c_current_hdemo_sk_node_7=[$3], c_current_addr_sk_node_7=[$4], c_first_shipto_date_sk_node_7=[$5], c_first_sales_date_sk_node_7=[$6], c_salutation_node_7=[$7], c_first_name_node_7=[$8], c_last_name_node_7=[$9], c_preferred_cust_flag_node_7=[$10], c_birth_day_node_7=[$11], c_birth_month_node_7=[$12], c_birth_year_node_7=[$13], c_birth_country_node_7=[$14], c_login_node_7=[$15], c_email_address_node_7=[$16], c_last_review_date_node_7=[$17], _c18=[_UTF-16LE'hello'])
      :  +- LogicalSort(sort0=[$2], dir0=[ASC])
      :     +- LogicalProject(c_customer_sk_node_7=[$0], c_customer_id_node_7=[$1], c_current_cdemo_sk_node_7=[$2], c_current_hdemo_sk_node_7=[$3], c_current_addr_sk_node_7=[$4], c_first_shipto_date_sk_node_7=[$5], c_first_sales_date_sk_node_7=[$6], c_salutation_node_7=[$7], c_first_name_node_7=[$8], c_last_name_node_7=[$9], c_preferred_cust_flag_node_7=[$10], c_birth_day_node_7=[$11], c_birth_month_node_7=[$12], c_birth_year_node_7=[$13], c_birth_country_node_7=[$14], c_login_node_7=[$15], c_email_address_node_7=[$16], c_last_review_date_node_7=[$17])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
      +- LogicalProject(uOYie=[AS($0, _UTF-16LE'uOYie')], cc_call_center_id_node_8=[$1], cc_rec_start_date_node_8=[$2], cc_rec_end_date_node_8=[$3], cc_closed_date_sk_node_8=[$4], cc_open_date_sk_node_8=[$5], cc_name_node_8=[$6], cc_class_node_8=[$7], cc_employees_node_8=[$8], cc_sq_ft_node_8=[$9], cc_hours_node_8=[$10], cc_manager_node_8=[$11], cc_mkt_id_node_8=[$12], cc_mkt_class_node_8=[$13], cc_mkt_desc_node_8=[$14], cc_market_manager_node_8=[$15], cc_division_node_8=[$16], cc_division_name_node_8=[$17], cc_company_node_8=[$18], cc_company_name_node_8=[$19], cc_street_number_node_8=[$20], cc_street_name_node_8=[$21], cc_street_type_node_8=[$22], cc_suite_number_node_8=[$23], cc_city_node_8=[$24], cc_county_node_8=[$25], cc_state_node_8=[$26], cc_zip_node_8=[$27], cc_country_node_8=[$28], cc_gmt_offset_node_8=[$29], cc_tax_percentage_node_8=[$30])
         +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30}])
            +- LogicalProject(cc_call_center_sk_node_8=[$0], cc_call_center_id_node_8=[$1], cc_rec_start_date_node_8=[$2], cc_rec_end_date_node_8=[$3], cc_closed_date_sk_node_8=[$4], cc_open_date_sk_node_8=[$5], cc_name_node_8=[$6], cc_class_node_8=[$7], cc_employees_node_8=[$8], cc_sq_ft_node_8=[$9], cc_hours_node_8=[$10], cc_manager_node_8=[$11], cc_mkt_id_node_8=[$12], cc_mkt_class_node_8=[$13], cc_mkt_desc_node_8=[$14], cc_market_manager_node_8=[$15], cc_division_node_8=[$16], cc_division_name_node_8=[$17], cc_company_node_8=[$18], cc_company_name_node_8=[$19], cc_street_number_node_8=[$20], cc_street_name_node_8=[$21], cc_street_type_node_8=[$22], cc_suite_number_node_8=[$23], cc_city_node_8=[$24], cc_county_node_8=[$25], cc_state_node_8=[$26], cc_zip_node_8=[$27], cc_country_node_8=[$28], cc_gmt_offset_node_8=[$29], cc_tax_percentage_node_8=[$30])
               +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cc_employees_node_8, 'hello' AS _c1])
+- SortAggregate(isMerge=[false], groupBy=[cc_employees_node_8], select=[cc_employees_node_8, MAX(cc_employees_node_8) AS EXPR$0])
   +- Sort(orderBy=[cc_employees_node_8 ASC])
      +- Exchange(distribution=[hash[cc_employees_node_8]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cc_mkt_id_node_8, c_current_cdemo_sk_node_7)], select=[c_customer_sk_node_7, c_customer_id_node_7, c_current_cdemo_sk_node_7, c_current_hdemo_sk_node_7, c_current_addr_sk_node_7, c_first_shipto_date_sk_node_7, c_first_sales_date_sk_node_7, c_salutation_node_7, c_first_name_node_7, c_last_name_node_7, c_preferred_cust_flag_node_7, c_birth_day_node_7, c_birth_month_node_7, c_birth_year_node_7, c_birth_country_node_7, c_login_node_7, c_email_address_node_7, c_last_review_date_node_7, _c18, uOYie, cc_call_center_id_node_8, cc_rec_start_date_node_8, cc_rec_end_date_node_8, cc_closed_date_sk_node_8, cc_open_date_sk_node_8, cc_name_node_8, cc_class_node_8, cc_employees_node_8, cc_sq_ft_node_8, cc_hours_node_8, cc_manager_node_8, cc_mkt_id_node_8, cc_mkt_class_node_8, cc_mkt_desc_node_8, cc_market_manager_node_8, cc_division_node_8, cc_division_name_node_8, cc_company_node_8, cc_company_name_node_8, cc_street_number_node_8, cc_street_name_node_8, cc_street_type_node_8, cc_suite_number_node_8, cc_city_node_8, cc_county_node_8, cc_state_node_8, cc_zip_node_8, cc_country_node_8, cc_gmt_offset_node_8, cc_tax_percentage_node_8], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[c_customer_sk AS c_customer_sk_node_7, c_customer_id AS c_customer_id_node_7, c_current_cdemo_sk AS c_current_cdemo_sk_node_7, c_current_hdemo_sk AS c_current_hdemo_sk_node_7, c_current_addr_sk AS c_current_addr_sk_node_7, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_7, c_first_sales_date_sk AS c_first_sales_date_sk_node_7, c_salutation AS c_salutation_node_7, c_first_name AS c_first_name_node_7, c_last_name AS c_last_name_node_7, c_preferred_cust_flag AS c_preferred_cust_flag_node_7, c_birth_day AS c_birth_day_node_7, c_birth_month AS c_birth_month_node_7, c_birth_year AS c_birth_year_node_7, c_birth_country AS c_birth_country_node_7, c_login AS c_login_node_7, c_email_address AS c_email_address_node_7, c_last_review_date AS c_last_review_date_node_7, 'hello' AS _c18])
            :     +- SortLimit(orderBy=[c_current_cdemo_sk ASC], offset=[0], fetch=[1], global=[true])
            :        +- Exchange(distribution=[single])
            :           +- SortLimit(orderBy=[c_current_cdemo_sk ASC], offset=[0], fetch=[1], global=[false])
            :              +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
            +- Calc(select=[cc_call_center_sk AS uOYie, cc_call_center_id AS cc_call_center_id_node_8, cc_rec_start_date AS cc_rec_start_date_node_8, cc_rec_end_date AS cc_rec_end_date_node_8, cc_closed_date_sk AS cc_closed_date_sk_node_8, cc_open_date_sk AS cc_open_date_sk_node_8, cc_name AS cc_name_node_8, cc_class AS cc_class_node_8, cc_employees AS cc_employees_node_8, cc_sq_ft AS cc_sq_ft_node_8, cc_hours AS cc_hours_node_8, cc_manager AS cc_manager_node_8, cc_mkt_id AS cc_mkt_id_node_8, cc_mkt_class AS cc_mkt_class_node_8, cc_mkt_desc AS cc_mkt_desc_node_8, cc_market_manager AS cc_market_manager_node_8, cc_division AS cc_division_node_8, cc_division_name AS cc_division_name_node_8, cc_company AS cc_company_node_8, cc_company_name AS cc_company_name_node_8, cc_street_number AS cc_street_number_node_8, cc_street_name AS cc_street_name_node_8, cc_street_type AS cc_street_type_node_8, cc_suite_number AS cc_suite_number_node_8, cc_city AS cc_city_node_8, cc_county AS cc_county_node_8, cc_state AS cc_state_node_8, cc_zip AS cc_zip_node_8, cc_country AS cc_country_node_8, cc_gmt_offset AS cc_gmt_offset_node_8, cc_tax_percentage AS cc_tax_percentage_node_8])
               +- SortAggregate(isMerge=[false], groupBy=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
                  +- Sort(orderBy=[cc_call_center_sk ASC, cc_call_center_id ASC, cc_rec_start_date ASC, cc_rec_end_date ASC, cc_closed_date_sk ASC, cc_open_date_sk ASC, cc_name ASC, cc_class ASC, cc_employees ASC, cc_sq_ft ASC, cc_hours ASC, cc_manager ASC, cc_mkt_id ASC, cc_mkt_class ASC, cc_mkt_desc ASC, cc_market_manager ASC, cc_division ASC, cc_division_name ASC, cc_company ASC, cc_company_name ASC, cc_street_number ASC, cc_street_name ASC, cc_street_type ASC, cc_suite_number ASC, cc_city ASC, cc_county ASC, cc_state ASC, cc_zip ASC, cc_country ASC, cc_gmt_offset ASC, cc_tax_percentage ASC])
                     +- Exchange(distribution=[hash[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage]])
                        +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cc_employees_node_8, 'hello' AS _c1])
+- SortAggregate(isMerge=[false], groupBy=[cc_employees_node_8], select=[cc_employees_node_8, MAX(cc_employees_node_8) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[cc_employees_node_8 ASC])
         +- Exchange(distribution=[hash[cc_employees_node_8]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(cc_mkt_id_node_8 = c_current_cdemo_sk_node_7)], select=[c_customer_sk_node_7, c_customer_id_node_7, c_current_cdemo_sk_node_7, c_current_hdemo_sk_node_7, c_current_addr_sk_node_7, c_first_shipto_date_sk_node_7, c_first_sales_date_sk_node_7, c_salutation_node_7, c_first_name_node_7, c_last_name_node_7, c_preferred_cust_flag_node_7, c_birth_day_node_7, c_birth_month_node_7, c_birth_year_node_7, c_birth_country_node_7, c_login_node_7, c_email_address_node_7, c_last_review_date_node_7, _c18, uOYie, cc_call_center_id_node_8, cc_rec_start_date_node_8, cc_rec_end_date_node_8, cc_closed_date_sk_node_8, cc_open_date_sk_node_8, cc_name_node_8, cc_class_node_8, cc_employees_node_8, cc_sq_ft_node_8, cc_hours_node_8, cc_manager_node_8, cc_mkt_id_node_8, cc_mkt_class_node_8, cc_mkt_desc_node_8, cc_market_manager_node_8, cc_division_node_8, cc_division_name_node_8, cc_company_node_8, cc_company_name_node_8, cc_street_number_node_8, cc_street_name_node_8, cc_street_type_node_8, cc_suite_number_node_8, cc_city_node_8, cc_county_node_8, cc_state_node_8, cc_zip_node_8, cc_country_node_8, cc_gmt_offset_node_8, cc_tax_percentage_node_8], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[c_customer_sk AS c_customer_sk_node_7, c_customer_id AS c_customer_id_node_7, c_current_cdemo_sk AS c_current_cdemo_sk_node_7, c_current_hdemo_sk AS c_current_hdemo_sk_node_7, c_current_addr_sk AS c_current_addr_sk_node_7, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_7, c_first_sales_date_sk AS c_first_sales_date_sk_node_7, c_salutation AS c_salutation_node_7, c_first_name AS c_first_name_node_7, c_last_name AS c_last_name_node_7, c_preferred_cust_flag AS c_preferred_cust_flag_node_7, c_birth_day AS c_birth_day_node_7, c_birth_month AS c_birth_month_node_7, c_birth_year AS c_birth_year_node_7, c_birth_country AS c_birth_country_node_7, c_login AS c_login_node_7, c_email_address AS c_email_address_node_7, c_last_review_date AS c_last_review_date_node_7, 'hello' AS _c18])
               :     +- SortLimit(orderBy=[c_current_cdemo_sk ASC], offset=[0], fetch=[1], global=[true])
               :        +- Exchange(distribution=[single])
               :           +- SortLimit(orderBy=[c_current_cdemo_sk ASC], offset=[0], fetch=[1], global=[false])
               :              +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
               +- Calc(select=[cc_call_center_sk AS uOYie, cc_call_center_id AS cc_call_center_id_node_8, cc_rec_start_date AS cc_rec_start_date_node_8, cc_rec_end_date AS cc_rec_end_date_node_8, cc_closed_date_sk AS cc_closed_date_sk_node_8, cc_open_date_sk AS cc_open_date_sk_node_8, cc_name AS cc_name_node_8, cc_class AS cc_class_node_8, cc_employees AS cc_employees_node_8, cc_sq_ft AS cc_sq_ft_node_8, cc_hours AS cc_hours_node_8, cc_manager AS cc_manager_node_8, cc_mkt_id AS cc_mkt_id_node_8, cc_mkt_class AS cc_mkt_class_node_8, cc_mkt_desc AS cc_mkt_desc_node_8, cc_market_manager AS cc_market_manager_node_8, cc_division AS cc_division_node_8, cc_division_name AS cc_division_name_node_8, cc_company AS cc_company_node_8, cc_company_name AS cc_company_name_node_8, cc_street_number AS cc_street_number_node_8, cc_street_name AS cc_street_name_node_8, cc_street_type AS cc_street_type_node_8, cc_suite_number AS cc_suite_number_node_8, cc_city AS cc_city_node_8, cc_county AS cc_county_node_8, cc_state AS cc_state_node_8, cc_zip AS cc_zip_node_8, cc_country AS cc_country_node_8, cc_gmt_offset AS cc_gmt_offset_node_8, cc_tax_percentage AS cc_tax_percentage_node_8])
                  +- SortAggregate(isMerge=[false], groupBy=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
                     +- Exchange(distribution=[forward])
                        +- Sort(orderBy=[cc_call_center_sk ASC, cc_call_center_id ASC, cc_rec_start_date ASC, cc_rec_end_date ASC, cc_closed_date_sk ASC, cc_open_date_sk ASC, cc_name ASC, cc_class ASC, cc_employees ASC, cc_sq_ft ASC, cc_hours ASC, cc_manager ASC, cc_mkt_id ASC, cc_mkt_class ASC, cc_mkt_desc ASC, cc_market_manager ASC, cc_division ASC, cc_division_name ASC, cc_company ASC, cc_company_name ASC, cc_street_number ASC, cc_street_name ASC, cc_street_type ASC, cc_suite_number ASC, cc_city ASC, cc_county ASC, cc_state ASC, cc_zip ASC, cc_country ASC, cc_gmt_offset ASC, cc_tax_percentage ASC])
                           +- Exchange(distribution=[hash[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage]])
                              +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0