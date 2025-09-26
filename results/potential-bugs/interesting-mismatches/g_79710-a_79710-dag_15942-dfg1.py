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
    return values.kurtosis()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_7 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_4 = autonode_6.order_by(col('c_birth_month_node_6'))
autonode_5 = autonode_7.filter(col('cd_dep_employed_count_node_7') > -16)
autonode_2 = autonode_4.filter(col('c_current_hdemo_sk_node_6') >= 11)
autonode_3 = autonode_5.order_by(col('cd_education_status_node_7'))
autonode_1 = autonode_2.join(autonode_3, col('cd_marital_status_node_7') == col('c_salutation_node_6'))
sink = autonode_1.group_by(col('c_salutation_node_6')).select(col('c_first_shipto_date_sk_node_6').max.alias('c_first_shipto_date_sk_node_6'))
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
      "error_message": "An error occurred while calling o43666146.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#86555724:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[12](input=RelSubset#86555722,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[12]), rel#86555721:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[12](input=RelSubset#86555720,groupBy=c_salutation,select=c_salutation, Partial_MAX(c_first_shipto_date_sk) AS max$0)]
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
LogicalProject(c_first_shipto_date_sk_node_6=[$1])
+- LogicalAggregate(group=[{7}], EXPR$0=[MAX($5)])
   +- LogicalJoin(condition=[=($20, $7)], joinType=[inner])
      :- LogicalFilter(condition=[>=($3, 11)])
      :  +- LogicalSort(sort0=[$12], dir0=[ASC])
      :     +- LogicalProject(c_customer_sk_node_6=[$0], c_customer_id_node_6=[$1], c_current_cdemo_sk_node_6=[$2], c_current_hdemo_sk_node_6=[$3], c_current_addr_sk_node_6=[$4], c_first_shipto_date_sk_node_6=[$5], c_first_sales_date_sk_node_6=[$6], c_salutation_node_6=[$7], c_first_name_node_6=[$8], c_last_name_node_6=[$9], c_preferred_cust_flag_node_6=[$10], c_birth_day_node_6=[$11], c_birth_month_node_6=[$12], c_birth_year_node_6=[$13], c_birth_country_node_6=[$14], c_login_node_6=[$15], c_email_address_node_6=[$16], c_last_review_date_node_6=[$17])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
      +- LogicalSort(sort0=[$3], dir0=[ASC])
         +- LogicalFilter(condition=[>($7, -16)])
            +- LogicalProject(cd_demo_sk_node_7=[$0], cd_gender_node_7=[$1], cd_marital_status_node_7=[$2], cd_education_status_node_7=[$3], cd_purchase_estimate_node_7=[$4], cd_credit_rating_node_7=[$5], cd_dep_count_node_7=[$6], cd_dep_employed_count_node_7=[$7], cd_dep_college_count_node_7=[$8])
               +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS c_first_shipto_date_sk_node_6])
+- SortAggregate(isMerge=[false], groupBy=[c_salutation], select=[c_salutation, MAX(c_first_shipto_date_sk) AS EXPR$0])
   +- Sort(orderBy=[c_salutation ASC])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cd_marital_status, c_salutation)], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[right])
         :- Exchange(distribution=[hash[c_salutation]])
         :  +- Calc(select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], where=[>=(c_current_hdemo_sk, 11)])
         :     +- SortLimit(orderBy=[c_birth_month ASC], offset=[0], fetch=[1], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[c_birth_month ASC], offset=[0], fetch=[1], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
         +- Exchange(distribution=[broadcast])
            +- SortLimit(orderBy=[cd_education_status ASC], offset=[0], fetch=[1], global=[true])
               +- Exchange(distribution=[single])
                  +- SortLimit(orderBy=[cd_education_status ASC], offset=[0], fetch=[1], global=[false])
                     +- Calc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[>(cd_dep_employed_count, -16)])
                        +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[>(cd_dep_employed_count, -16)]]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS c_first_shipto_date_sk_node_6])
+- SortAggregate(isMerge=[false], groupBy=[c_salutation], select=[c_salutation, MAX(c_first_shipto_date_sk) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[c_salutation ASC])
         +- Exchange(distribution=[keep_input_as_is[hash[c_salutation]]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(cd_marital_status = c_salutation)], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[right])
               :- Exchange(distribution=[hash[c_salutation]])
               :  +- Calc(select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], where=[(c_current_hdemo_sk >= 11)])
               :     +- SortLimit(orderBy=[c_birth_month ASC], offset=[0], fetch=[1], global=[true])
               :        +- Exchange(distribution=[single])
               :           +- SortLimit(orderBy=[c_birth_month ASC], offset=[0], fetch=[1], global=[false])
               :              +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
               +- Exchange(distribution=[broadcast])
                  +- SortLimit(orderBy=[cd_education_status ASC], offset=[0], fetch=[1], global=[true])
                     +- Exchange(distribution=[single])
                        +- SortLimit(orderBy=[cd_education_status ASC], offset=[0], fetch=[1], global=[false])
                           +- Calc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[(cd_dep_employed_count > -16)])
                              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[>(cd_dep_employed_count, -16)]]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0