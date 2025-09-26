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
    return len(values)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_13 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_12 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_11 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_6 = autonode_10.alias('lvmqb')
autonode_9 = autonode_13.filter(col('t_second_node_13') < 0)
autonode_8 = autonode_12.select(col('d_following_holiday_node_12'))
autonode_7 = autonode_11.select(col('c_current_addr_sk_node_11'))
autonode_5 = autonode_8.join(autonode_9, col('d_following_holiday_node_12') == col('t_shift_node_13'))
autonode_4 = autonode_6.join(autonode_7, col('c_current_addr_sk_node_11') == col('c_birth_month_node_10'))
autonode_3 = autonode_5.order_by(col('t_time_node_13'))
autonode_2 = autonode_4.limit(41)
autonode_1 = autonode_2.join(autonode_3, col('t_time_id_node_13') == col('c_email_address_node_10'))
sink = autonode_1.group_by(col('c_current_cdemo_sk_node_10')).select(col('c_salutation_node_10').min.alias('c_salutation_node_10'))
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
      "error_message": "An error occurred while calling o136353569.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#275397627:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[3](input=RelSubset#275397625,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[3]), rel#275397624:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#275397623,groupBy=t_time_id,select=t_time_id)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (3) must be less than size (1)
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
LogicalProject(c_salutation_node_10=[$1])
+- LogicalAggregate(group=[{2}], EXPR$0=[MIN($7)])
   +- LogicalJoin(condition=[=($21, $16)], joinType=[inner])
      :- LogicalSort(fetch=[41])
      :  +- LogicalJoin(condition=[=($18, $12)], joinType=[inner])
      :     :- LogicalProject(lvmqb=[AS($0, _UTF-16LE'lvmqb')], c_customer_id_node_10=[$1], c_current_cdemo_sk_node_10=[$2], c_current_hdemo_sk_node_10=[$3], c_current_addr_sk_node_10=[$4], c_first_shipto_date_sk_node_10=[$5], c_first_sales_date_sk_node_10=[$6], c_salutation_node_10=[$7], c_first_name_node_10=[$8], c_last_name_node_10=[$9], c_preferred_cust_flag_node_10=[$10], c_birth_day_node_10=[$11], c_birth_month_node_10=[$12], c_birth_year_node_10=[$13], c_birth_country_node_10=[$14], c_login_node_10=[$15], c_email_address_node_10=[$16], c_last_review_date_node_10=[$17])
      :     :  +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
      :     +- LogicalProject(c_current_addr_sk_node_11=[$4])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
      +- LogicalSort(sort0=[$3], dir0=[ASC])
         +- LogicalJoin(condition=[=($0, $8)], joinType=[inner])
            :- LogicalProject(d_following_holiday_node_12=[$18])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
            +- LogicalFilter(condition=[<($5, 0)])
               +- LogicalProject(t_time_sk_node_13=[$0], t_time_id_node_13=[$1], t_time_node_13=[$2], t_hour_node_13=[$3], t_minute_node_13=[$4], t_second_node_13=[$5], t_am_pm_node_13=[$6], t_shift_node_13=[$7], t_sub_shift_node_13=[$8], t_meal_time_node_13=[$9])
                  +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS c_salutation_node_10])
+- SortAggregate(isMerge=[true], groupBy=[c_current_cdemo_sk_node_10], select=[c_current_cdemo_sk_node_10, Final_MIN(min$0) AS EXPR$0])
   +- Sort(orderBy=[c_current_cdemo_sk_node_10 ASC])
      +- Exchange(distribution=[hash[c_current_cdemo_sk_node_10]])
         +- LocalSortAggregate(groupBy=[c_current_cdemo_sk_node_10], select=[c_current_cdemo_sk_node_10, Partial_MIN(c_salutation_node_10) AS min$0])
            +- Sort(orderBy=[c_current_cdemo_sk_node_10 ASC])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(t_time_id, c_email_address_node_10)], select=[lvmqb, c_customer_id_node_10, c_current_cdemo_sk_node_10, c_current_hdemo_sk_node_10, c_current_addr_sk_node_10, c_first_shipto_date_sk_node_10, c_first_sales_date_sk_node_10, c_salutation_node_10, c_first_name_node_10, c_last_name_node_10, c_preferred_cust_flag_node_10, c_birth_day_node_10, c_birth_month_node_10, c_birth_year_node_10, c_birth_country_node_10, c_login_node_10, c_email_address_node_10, c_last_review_date_node_10, c_current_addr_sk, d_following_holiday, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], build=[right])
                  :- Limit(offset=[0], fetch=[41], global=[true])
                  :  +- Exchange(distribution=[single])
                  :     +- Limit(offset=[0], fetch=[41], global=[false])
                  :        +- HashJoin(joinType=[InnerJoin], where=[=(c_current_addr_sk, c_birth_month_node_10)], select=[lvmqb, c_customer_id_node_10, c_current_cdemo_sk_node_10, c_current_hdemo_sk_node_10, c_current_addr_sk_node_10, c_first_shipto_date_sk_node_10, c_first_sales_date_sk_node_10, c_salutation_node_10, c_first_name_node_10, c_last_name_node_10, c_preferred_cust_flag_node_10, c_birth_day_node_10, c_birth_month_node_10, c_birth_year_node_10, c_birth_country_node_10, c_login_node_10, c_email_address_node_10, c_last_review_date_node_10, c_current_addr_sk], build=[right])
                  :           :- Exchange(distribution=[hash[c_birth_month_node_10]])
                  :           :  +- Calc(select=[c_customer_sk AS lvmqb, c_customer_id AS c_customer_id_node_10, c_current_cdemo_sk AS c_current_cdemo_sk_node_10, c_current_hdemo_sk AS c_current_hdemo_sk_node_10, c_current_addr_sk AS c_current_addr_sk_node_10, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_10, c_first_sales_date_sk AS c_first_sales_date_sk_node_10, c_salutation AS c_salutation_node_10, c_first_name AS c_first_name_node_10, c_last_name AS c_last_name_node_10, c_preferred_cust_flag AS c_preferred_cust_flag_node_10, c_birth_day AS c_birth_day_node_10, c_birth_month AS c_birth_month_node_10, c_birth_year AS c_birth_year_node_10, c_birth_country AS c_birth_country_node_10, c_login AS c_login_node_10, c_email_address AS c_email_address_node_10, c_last_review_date AS c_last_review_date_node_10])
                  :           :     +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                  :           +- Exchange(distribution=[hash[c_current_addr_sk]])
                  :              +- TableSourceScan(table=[[default_catalog, default_database, customer, project=[c_current_addr_sk], metadata=[]]], fields=[c_current_addr_sk])
                  +- Exchange(distribution=[broadcast])
                     +- SortLimit(orderBy=[t_time ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[t_time ASC], offset=[0], fetch=[1], global=[false])
                              +- HashJoin(joinType=[InnerJoin], where=[=(d_following_holiday, t_shift)], select=[d_following_holiday, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], build=[left])
                                 :- Exchange(distribution=[hash[d_following_holiday]])
                                 :  +- TableSourceScan(table=[[default_catalog, default_database, date_dim, project=[d_following_holiday], metadata=[]]], fields=[d_following_holiday])
                                 +- Exchange(distribution=[hash[t_shift]])
                                    +- Calc(select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], where=[<(t_second, 0)])
                                       +- TableSourceScan(table=[[default_catalog, default_database, time_dim, filter=[<(t_second, 0)]]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS c_salutation_node_10])
+- SortAggregate(isMerge=[true], groupBy=[c_current_cdemo_sk_node_10], select=[c_current_cdemo_sk_node_10, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[c_current_cdemo_sk_node_10 ASC])
         +- Exchange(distribution=[hash[c_current_cdemo_sk_node_10]])
            +- LocalSortAggregate(groupBy=[c_current_cdemo_sk_node_10], select=[c_current_cdemo_sk_node_10, Partial_MIN(c_salutation_node_10) AS min$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[c_current_cdemo_sk_node_10 ASC])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(t_time_id = c_email_address_node_10)], select=[lvmqb, c_customer_id_node_10, c_current_cdemo_sk_node_10, c_current_hdemo_sk_node_10, c_current_addr_sk_node_10, c_first_shipto_date_sk_node_10, c_first_sales_date_sk_node_10, c_salutation_node_10, c_first_name_node_10, c_last_name_node_10, c_preferred_cust_flag_node_10, c_birth_day_node_10, c_birth_month_node_10, c_birth_year_node_10, c_birth_country_node_10, c_login_node_10, c_email_address_node_10, c_last_review_date_node_10, c_current_addr_sk, d_following_holiday, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], build=[right])
                        :- Limit(offset=[0], fetch=[41], global=[true])
                        :  +- Exchange(distribution=[single])
                        :     +- Limit(offset=[0], fetch=[41], global=[false])
                        :        +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(c_current_addr_sk = c_birth_month_node_10)], select=[lvmqb, c_customer_id_node_10, c_current_cdemo_sk_node_10, c_current_hdemo_sk_node_10, c_current_addr_sk_node_10, c_first_shipto_date_sk_node_10, c_first_sales_date_sk_node_10, c_salutation_node_10, c_first_name_node_10, c_last_name_node_10, c_preferred_cust_flag_node_10, c_birth_day_node_10, c_birth_month_node_10, c_birth_year_node_10, c_birth_country_node_10, c_login_node_10, c_email_address_node_10, c_last_review_date_node_10, c_current_addr_sk], build=[right])
                        :           :- Exchange(distribution=[hash[c_birth_month_node_10]])
                        :           :  +- Calc(select=[c_customer_sk AS lvmqb, c_customer_id AS c_customer_id_node_10, c_current_cdemo_sk AS c_current_cdemo_sk_node_10, c_current_hdemo_sk AS c_current_hdemo_sk_node_10, c_current_addr_sk AS c_current_addr_sk_node_10, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_10, c_first_sales_date_sk AS c_first_sales_date_sk_node_10, c_salutation AS c_salutation_node_10, c_first_name AS c_first_name_node_10, c_last_name AS c_last_name_node_10, c_preferred_cust_flag AS c_preferred_cust_flag_node_10, c_birth_day AS c_birth_day_node_10, c_birth_month AS c_birth_month_node_10, c_birth_year AS c_birth_year_node_10, c_birth_country AS c_birth_country_node_10, c_login AS c_login_node_10, c_email_address AS c_email_address_node_10, c_last_review_date AS c_last_review_date_node_10])
                        :           :     +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                        :           +- Exchange(distribution=[hash[c_current_addr_sk]])
                        :              +- TableSourceScan(table=[[default_catalog, default_database, customer, project=[c_current_addr_sk], metadata=[]]], fields=[c_current_addr_sk])
                        +- Exchange(distribution=[broadcast])
                           +- SortLimit(orderBy=[t_time ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[t_time ASC], offset=[0], fetch=[1], global=[false])
                                    +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(d_following_holiday = t_shift)], select=[d_following_holiday, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], build=[left])
                                       :- Exchange(distribution=[hash[d_following_holiday]])
                                       :  +- TableSourceScan(table=[[default_catalog, default_database, date_dim, project=[d_following_holiday], metadata=[]]], fields=[d_following_holiday])
                                       +- Exchange(distribution=[hash[t_shift]])
                                          +- Calc(select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], where=[(t_second < 0)])
                                             +- TableSourceScan(table=[[default_catalog, default_database, time_dim, filter=[<(t_second, 0)]]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0