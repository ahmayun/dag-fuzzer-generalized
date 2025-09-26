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
    return values.product()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_9 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_8 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_11 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_6 = autonode_10.distinct()
autonode_5 = autonode_8.join(autonode_9, col('i_item_sk_node_9') == col('wp_char_count_node_8'))
autonode_7 = autonode_11.order_by(col('cd_purchase_estimate_node_11'))
autonode_3 = autonode_5.add_columns(lit("hello"))
autonode_4 = autonode_6.join(autonode_7, col('c_last_name_node_10') == col('cd_education_status_node_11'))
autonode_1 = autonode_3.select(col('wp_web_page_sk_node_8'))
autonode_2 = autonode_4.group_by(col('c_birth_month_node_10')).select(col('c_birth_month_node_10').min.alias('c_birth_month_node_10'))
sink = autonode_1.join(autonode_2, col('wp_web_page_sk_node_8') == col('c_birth_month_node_10'))
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
      "error_message": "An error occurred while calling o327437833.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#660446616:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[4](input=RelSubset#660446614,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[4]), rel#660446613:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[4](input=RelSubset#660446612,groupBy=cd_education_status,select=cd_education_status)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (4) must be less than size (1)
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
LogicalJoin(condition=[=($0, $1)], joinType=[inner])
:- LogicalProject(wp_web_page_sk_node_8=[$0])
:  +- LogicalJoin(condition=[=($14, $10)], joinType=[inner])
:     :- LogicalProject(wp_web_page_sk_node_8=[$0], wp_web_page_id_node_8=[$1], wp_rec_start_date_node_8=[$2], wp_rec_end_date_node_8=[$3], wp_creation_date_sk_node_8=[$4], wp_access_date_sk_node_8=[$5], wp_autogen_flag_node_8=[$6], wp_customer_sk_node_8=[$7], wp_url_node_8=[$8], wp_type_node_8=[$9], wp_char_count_node_8=[$10], wp_link_count_node_8=[$11], wp_image_count_node_8=[$12], wp_max_ad_count_node_8=[$13])
:     :  +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
:     +- LogicalProject(i_item_sk_node_9=[$0], i_item_id_node_9=[$1], i_rec_start_date_node_9=[$2], i_rec_end_date_node_9=[$3], i_item_desc_node_9=[$4], i_current_price_node_9=[$5], i_wholesale_cost_node_9=[$6], i_brand_id_node_9=[$7], i_brand_node_9=[$8], i_class_id_node_9=[$9], i_class_node_9=[$10], i_category_id_node_9=[$11], i_category_node_9=[$12], i_manufact_id_node_9=[$13], i_manufact_node_9=[$14], i_size_node_9=[$15], i_formulation_node_9=[$16], i_color_node_9=[$17], i_units_node_9=[$18], i_container_node_9=[$19], i_manager_id_node_9=[$20], i_product_name_node_9=[$21])
:        +- LogicalTableScan(table=[[default_catalog, default_database, item]])
+- LogicalProject(c_birth_month_node_10=[$1])
   +- LogicalAggregate(group=[{12}], EXPR$0=[MIN($12)])
      +- LogicalJoin(condition=[=($9, $21)], joinType=[inner])
         :- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}])
         :  +- LogicalProject(c_customer_sk_node_10=[$0], c_customer_id_node_10=[$1], c_current_cdemo_sk_node_10=[$2], c_current_hdemo_sk_node_10=[$3], c_current_addr_sk_node_10=[$4], c_first_shipto_date_sk_node_10=[$5], c_first_sales_date_sk_node_10=[$6], c_salutation_node_10=[$7], c_first_name_node_10=[$8], c_last_name_node_10=[$9], c_preferred_cust_flag_node_10=[$10], c_birth_day_node_10=[$11], c_birth_month_node_10=[$12], c_birth_year_node_10=[$13], c_birth_country_node_10=[$14], c_login_node_10=[$15], c_email_address_node_10=[$16], c_last_review_date_node_10=[$17])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
         +- LogicalSort(sort0=[$4], dir0=[ASC])
            +- LogicalProject(cd_demo_sk_node_11=[$0], cd_gender_node_11=[$1], cd_marital_status_node_11=[$2], cd_education_status_node_11=[$3], cd_purchase_estimate_node_11=[$4], cd_credit_rating_node_11=[$5], cd_dep_count_node_11=[$6], cd_dep_employed_count_node_11=[$7], cd_dep_college_count_node_11=[$8])
               +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])

== Optimized Physical Plan ==
HashJoin(joinType=[InnerJoin], where=[=(wp_web_page_sk_node_8, c_birth_month_node_10)], select=[wp_web_page_sk_node_8, c_birth_month_node_10], isBroadcast=[true], build=[right])
:- Calc(select=[wp_web_page_sk AS wp_web_page_sk_node_8])
:  +- HashJoin(joinType=[InnerJoin], where=[=(i_item_sk, wp_char_count)], select=[wp_web_page_sk, wp_char_count, i_item_sk], isBroadcast=[true], build=[left])
:     :- Exchange(distribution=[broadcast])
:     :  +- TableSourceScan(table=[[default_catalog, default_database, web_page, project=[wp_web_page_sk, wp_char_count], metadata=[]]], fields=[wp_web_page_sk, wp_char_count])
:     +- TableSourceScan(table=[[default_catalog, default_database, item, project=[i_item_sk], metadata=[]]], fields=[i_item_sk])
+- Exchange(distribution=[broadcast])
   +- Calc(select=[EXPR$0 AS c_birth_month_node_10])
      +- HashAggregate(isMerge=[true], groupBy=[c_birth_month], select=[c_birth_month, Final_MIN(min$0) AS EXPR$0])
         +- Exchange(distribution=[hash[c_birth_month]])
            +- LocalHashAggregate(groupBy=[c_birth_month], select=[c_birth_month, Partial_MIN(c_birth_month) AS min$0])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(c_last_name, cd_education_status)], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[right])
                  :- HashAggregate(isMerge=[false], groupBy=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                  :  +- Exchange(distribution=[hash[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date]])
                  :     +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                  +- Exchange(distribution=[broadcast])
                     +- SortLimit(orderBy=[cd_purchase_estimate ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[cd_purchase_estimate ASC], offset=[0], fetch=[1], global=[false])
                              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

== Optimized Execution Plan ==
HashJoin(joinType=[InnerJoin], where=[(wp_web_page_sk_node_8 = c_birth_month_node_10)], select=[wp_web_page_sk_node_8, c_birth_month_node_10], isBroadcast=[true], build=[right])
:- Calc(select=[wp_web_page_sk AS wp_web_page_sk_node_8])
:  +- HashJoin(joinType=[InnerJoin], where=[(i_item_sk = wp_char_count)], select=[wp_web_page_sk, wp_char_count, i_item_sk], isBroadcast=[true], build=[left])
:     :- Exchange(distribution=[broadcast])
:     :  +- TableSourceScan(table=[[default_catalog, default_database, web_page, project=[wp_web_page_sk, wp_char_count], metadata=[]]], fields=[wp_web_page_sk, wp_char_count])
:     +- TableSourceScan(table=[[default_catalog, default_database, item, project=[i_item_sk], metadata=[]]], fields=[i_item_sk])
+- Exchange(distribution=[broadcast])
   +- Calc(select=[EXPR$0 AS c_birth_month_node_10])
      +- HashAggregate(isMerge=[true], groupBy=[c_birth_month], select=[c_birth_month, Final_MIN(min$0) AS EXPR$0])
         +- Exchange(distribution=[hash[c_birth_month]])
            +- LocalHashAggregate(groupBy=[c_birth_month], select=[c_birth_month, Partial_MIN(c_birth_month) AS min$0])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[(c_last_name = cd_education_status)], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[right])
                  :- HashAggregate(isMerge=[false], groupBy=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                  :  +- Exchange(distribution=[hash[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date]])
                  :     +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                  +- Exchange(distribution=[broadcast])
                     +- SortLimit(orderBy=[cd_purchase_estimate ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[cd_purchase_estimate ASC], offset=[0], fetch=[1], global=[false])
                              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0