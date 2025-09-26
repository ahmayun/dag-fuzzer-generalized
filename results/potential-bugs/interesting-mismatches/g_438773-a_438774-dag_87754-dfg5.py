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

autonode_6 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_7 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_4 = autonode_6.filter(col('web_mkt_class_node_6').char_length >= 5)
autonode_5 = autonode_7.order_by(col('c_birth_year_node_7'))
autonode_3 = autonode_4.join(autonode_5, col('c_current_cdemo_sk_node_7') == col('web_open_date_sk_node_6'))
autonode_2 = autonode_3.group_by(col('web_tax_percentage_node_6')).select(col('web_mkt_class_node_6').max.alias('web_mkt_class_node_6'))
autonode_1 = autonode_2.distinct()
sink = autonode_1.limit(60)
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
      "error_message": "An error occurred while calling o238907545.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#483373172:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[13](input=RelSubset#483373170,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[13]), rel#483373169:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[13](input=RelSubset#483373168,groupBy=c_current_cdemo_sk,select=c_current_cdemo_sk)]
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
LogicalSort(fetch=[60])
+- LogicalAggregate(group=[{0}])
   +- LogicalProject(web_mkt_class_node_6=[$1])
      +- LogicalAggregate(group=[{25}], EXPR$0=[MAX($10)])
         +- LogicalJoin(condition=[=($28, $5)], joinType=[inner])
            :- LogicalFilter(condition=[>=(CHAR_LENGTH($10), 5)])
            :  +- LogicalProject(web_site_sk_node_6=[$0], web_site_id_node_6=[$1], web_rec_start_date_node_6=[$2], web_rec_end_date_node_6=[$3], web_name_node_6=[$4], web_open_date_sk_node_6=[$5], web_close_date_sk_node_6=[$6], web_class_node_6=[$7], web_manager_node_6=[$8], web_mkt_id_node_6=[$9], web_mkt_class_node_6=[$10], web_mkt_desc_node_6=[$11], web_market_manager_node_6=[$12], web_company_id_node_6=[$13], web_company_name_node_6=[$14], web_street_number_node_6=[$15], web_street_name_node_6=[$16], web_street_type_node_6=[$17], web_suite_number_node_6=[$18], web_city_node_6=[$19], web_county_node_6=[$20], web_state_node_6=[$21], web_zip_node_6=[$22], web_country_node_6=[$23], web_gmt_offset_node_6=[$24], web_tax_percentage_node_6=[$25])
            :     +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])
            +- LogicalSort(sort0=[$13], dir0=[ASC])
               +- LogicalProject(c_customer_sk_node_7=[$0], c_customer_id_node_7=[$1], c_current_cdemo_sk_node_7=[$2], c_current_hdemo_sk_node_7=[$3], c_current_addr_sk_node_7=[$4], c_first_shipto_date_sk_node_7=[$5], c_first_sales_date_sk_node_7=[$6], c_salutation_node_7=[$7], c_first_name_node_7=[$8], c_last_name_node_7=[$9], c_preferred_cust_flag_node_7=[$10], c_birth_day_node_7=[$11], c_birth_month_node_7=[$12], c_birth_year_node_7=[$13], c_birth_country_node_7=[$14], c_login_node_7=[$15], c_email_address_node_7=[$16], c_last_review_date_node_7=[$17])
                  +- LogicalTableScan(table=[[default_catalog, default_database, customer]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[60], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[60], global=[false])
      +- SortAggregate(isMerge=[true], groupBy=[web_mkt_class_node_6], select=[web_mkt_class_node_6])
         +- Sort(orderBy=[web_mkt_class_node_6 ASC])
            +- Exchange(distribution=[hash[web_mkt_class_node_6]])
               +- LocalSortAggregate(groupBy=[web_mkt_class_node_6], select=[web_mkt_class_node_6])
                  +- Sort(orderBy=[web_mkt_class_node_6 ASC])
                     +- Calc(select=[EXPR$0 AS web_mkt_class_node_6])
                        +- SortAggregate(isMerge=[false], groupBy=[web_tax_percentage], select=[web_tax_percentage, MAX(web_mkt_class) AS EXPR$0])
                           +- Sort(orderBy=[web_tax_percentage ASC])
                              +- Exchange(distribution=[hash[web_tax_percentage]])
                                 +- NestedLoopJoin(joinType=[InnerJoin], where=[=(c_current_cdemo_sk, web_open_date_sk)], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[right])
                                    :- Calc(select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], where=[>=(CHAR_LENGTH(web_mkt_class), 5)])
                                    :  +- TableSourceScan(table=[[default_catalog, default_database, web_site, filter=[>=(CHAR_LENGTH(web_mkt_class), 5)]]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                                    +- Exchange(distribution=[broadcast])
                                       +- SortLimit(orderBy=[c_birth_year ASC], offset=[0], fetch=[1], global=[true])
                                          +- Exchange(distribution=[single])
                                             +- SortLimit(orderBy=[c_birth_year ASC], offset=[0], fetch=[1], global=[false])
                                                +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[60], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[60], global=[false])
      +- SortAggregate(isMerge=[true], groupBy=[web_mkt_class_node_6], select=[web_mkt_class_node_6])
         +- Exchange(distribution=[forward])
            +- Sort(orderBy=[web_mkt_class_node_6 ASC])
               +- Exchange(distribution=[hash[web_mkt_class_node_6]])
                  +- LocalSortAggregate(groupBy=[web_mkt_class_node_6], select=[web_mkt_class_node_6])
                     +- Exchange(distribution=[forward])
                        +- Sort(orderBy=[web_mkt_class_node_6 ASC])
                           +- Calc(select=[EXPR$0 AS web_mkt_class_node_6])
                              +- SortAggregate(isMerge=[false], groupBy=[web_tax_percentage], select=[web_tax_percentage, MAX(web_mkt_class) AS EXPR$0])
                                 +- Exchange(distribution=[forward])
                                    +- Sort(orderBy=[web_tax_percentage ASC])
                                       +- Exchange(distribution=[hash[web_tax_percentage]])
                                          +- NestedLoopJoin(joinType=[InnerJoin], where=[(c_current_cdemo_sk = web_open_date_sk)], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[right])
                                             :- Calc(select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], where=[(CHAR_LENGTH(web_mkt_class) >= 5)])
                                             :  +- TableSourceScan(table=[[default_catalog, default_database, web_site, filter=[>=(CHAR_LENGTH(web_mkt_class), 5)]]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                                             +- Exchange(distribution=[broadcast])
                                                +- SortLimit(orderBy=[c_birth_year ASC], offset=[0], fetch=[1], global=[true])
                                                   +- Exchange(distribution=[single])
                                                      +- SortLimit(orderBy=[c_birth_year ASC], offset=[0], fetch=[1], global=[false])
                                                         +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0