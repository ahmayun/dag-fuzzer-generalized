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

autonode_6 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_8 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_7 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_4 = autonode_6.group_by(col('inv_date_sk_node_6')).select(col('inv_date_sk_node_6').count.alias('inv_date_sk_node_6'))
autonode_5 = autonode_7.join(autonode_8, col('c_current_addr_sk_node_8') == col('hd_income_band_sk_node_7'))
autonode_2 = autonode_4.distinct()
autonode_3 = autonode_5.order_by(col('c_birth_year_node_8'))
autonode_1 = autonode_2.join(autonode_3, col('c_current_addr_sk_node_8') == col('inv_date_sk_node_6'))
sink = autonode_1.group_by(col('c_current_hdemo_sk_node_8')).select(col('c_customer_sk_node_8').min.alias('c_customer_sk_node_8'))
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
LogicalProject(c_customer_sk_node_8=[$1])
+- LogicalAggregate(group=[{9}], EXPR$0=[MIN($6)])
   +- LogicalJoin(condition=[=($10, $0)], joinType=[inner])
      :- LogicalAggregate(group=[{0}])
      :  +- LogicalProject(inv_date_sk_node_6=[$1])
      :     +- LogicalAggregate(group=[{0}], EXPR$0=[COUNT($0)])
      :        +- LogicalProject(inv_date_sk_node_6=[$0])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])
      +- LogicalSort(sort0=[$18], dir0=[ASC])
         +- LogicalJoin(condition=[=($9, $1)], joinType=[inner])
            :- LogicalProject(hd_demo_sk_node_7=[$0], hd_income_band_sk_node_7=[$1], hd_buy_potential_node_7=[$2], hd_dep_count_node_7=[$3], hd_vehicle_count_node_7=[$4])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
            +- LogicalProject(c_customer_sk_node_8=[$0], c_customer_id_node_8=[$1], c_current_cdemo_sk_node_8=[$2], c_current_hdemo_sk_node_8=[$3], c_current_addr_sk_node_8=[$4], c_first_shipto_date_sk_node_8=[$5], c_first_sales_date_sk_node_8=[$6], c_salutation_node_8=[$7], c_first_name_node_8=[$8], c_last_name_node_8=[$9], c_preferred_cust_flag_node_8=[$10], c_birth_day_node_8=[$11], c_birth_month_node_8=[$12], c_birth_year_node_8=[$13], c_birth_country_node_8=[$14], c_login_node_8=[$15], c_email_address_node_8=[$16], c_last_review_date_node_8=[$17])
               +- LogicalTableScan(table=[[default_catalog, default_database, customer]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS c_customer_sk_node_8])
+- HashAggregate(isMerge=[true], groupBy=[c_current_hdemo_sk_node_8], select=[c_current_hdemo_sk_node_8, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[c_current_hdemo_sk_node_8]])
      +- LocalHashAggregate(groupBy=[c_current_hdemo_sk_node_8], select=[c_current_hdemo_sk_node_8, Partial_MIN(c_customer_sk_node_8) AS min$0])
         +- Calc(select=[inv_date_sk_node_6, hd_demo_sk_node_7, hd_income_band_sk_node_7, hd_buy_potential_node_7, hd_dep_count_node_7, hd_vehicle_count_node_7, c_customer_sk_node_8, c_customer_id_node_8, c_current_cdemo_sk_node_8, c_current_hdemo_sk_node_8, c_current_addr_sk_node_8, c_first_shipto_date_sk_node_8, c_first_sales_date_sk_node_8, c_salutation_node_8, c_first_name_node_8, c_last_name_node_8, c_preferred_cust_flag_node_8, c_birth_day_node_8, c_birth_month_node_8, c_birth_year_node_8, c_birth_country_node_8, c_login_node_8, c_email_address_node_8, c_last_review_date_node_8])
            +- HashJoin(joinType=[InnerJoin], where=[=(c_current_addr_sk_node_80, inv_date_sk_node_6)], select=[inv_date_sk_node_6, hd_demo_sk_node_7, hd_income_band_sk_node_7, hd_buy_potential_node_7, hd_dep_count_node_7, hd_vehicle_count_node_7, c_customer_sk_node_8, c_customer_id_node_8, c_current_cdemo_sk_node_8, c_current_hdemo_sk_node_8, c_current_addr_sk_node_8, c_first_shipto_date_sk_node_8, c_first_sales_date_sk_node_8, c_salutation_node_8, c_first_name_node_8, c_last_name_node_8, c_preferred_cust_flag_node_8, c_birth_day_node_8, c_birth_month_node_8, c_birth_year_node_8, c_birth_country_node_8, c_login_node_8, c_email_address_node_8, c_last_review_date_node_8, c_current_addr_sk_node_80], isBroadcast=[true], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- HashAggregate(isMerge=[true], groupBy=[inv_date_sk_node_6], select=[inv_date_sk_node_6])
               :     +- Exchange(distribution=[hash[inv_date_sk_node_6]])
               :        +- LocalHashAggregate(groupBy=[inv_date_sk_node_6], select=[inv_date_sk_node_6])
               :           +- Calc(select=[EXPR$0 AS inv_date_sk_node_6])
               :              +- HashAggregate(isMerge=[true], groupBy=[inv_date_sk], select=[inv_date_sk, Final_COUNT(count$0) AS EXPR$0])
               :                 +- Exchange(distribution=[hash[inv_date_sk]])
               :                    +- LocalHashAggregate(groupBy=[inv_date_sk], select=[inv_date_sk, Partial_COUNT(inv_date_sk) AS count$0])
               :                       +- TableSourceScan(table=[[default_catalog, default_database, inventory, project=[inv_date_sk], metadata=[]]], fields=[inv_date_sk])
               +- Calc(select=[hd_demo_sk AS hd_demo_sk_node_7, hd_income_band_sk AS hd_income_band_sk_node_7, hd_buy_potential AS hd_buy_potential_node_7, hd_dep_count AS hd_dep_count_node_7, hd_vehicle_count AS hd_vehicle_count_node_7, c_customer_sk AS c_customer_sk_node_8, c_customer_id AS c_customer_id_node_8, c_current_cdemo_sk AS c_current_cdemo_sk_node_8, c_current_hdemo_sk AS c_current_hdemo_sk_node_8, c_current_addr_sk AS c_current_addr_sk_node_8, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_8, c_first_sales_date_sk AS c_first_sales_date_sk_node_8, c_salutation AS c_salutation_node_8, c_first_name AS c_first_name_node_8, c_last_name AS c_last_name_node_8, c_preferred_cust_flag AS c_preferred_cust_flag_node_8, c_birth_day AS c_birth_day_node_8, c_birth_month AS c_birth_month_node_8, c_birth_year AS c_birth_year_node_8, c_birth_country AS c_birth_country_node_8, c_login AS c_login_node_8, c_email_address AS c_email_address_node_8, c_last_review_date AS c_last_review_date_node_8, CAST(c_current_addr_sk AS BIGINT) AS c_current_addr_sk_node_80])
                  +- Sort(orderBy=[c_birth_year ASC])
                     +- Exchange(distribution=[single])
                        +- HashJoin(joinType=[InnerJoin], where=[=(c_current_addr_sk, hd_income_band_sk)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], isBroadcast=[true], build=[left])
                           :- Exchange(distribution=[broadcast])
                           :  +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                           +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS c_customer_sk_node_8])
+- HashAggregate(isMerge=[true], groupBy=[c_current_hdemo_sk_node_8], select=[c_current_hdemo_sk_node_8, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[c_current_hdemo_sk_node_8]])
      +- LocalHashAggregate(groupBy=[c_current_hdemo_sk_node_8], select=[c_current_hdemo_sk_node_8, Partial_MIN(c_customer_sk_node_8) AS min$0])
         +- Calc(select=[inv_date_sk_node_6, hd_demo_sk_node_7, hd_income_band_sk_node_7, hd_buy_potential_node_7, hd_dep_count_node_7, hd_vehicle_count_node_7, c_customer_sk_node_8, c_customer_id_node_8, c_current_cdemo_sk_node_8, c_current_hdemo_sk_node_8, c_current_addr_sk_node_8, c_first_shipto_date_sk_node_8, c_first_sales_date_sk_node_8, c_salutation_node_8, c_first_name_node_8, c_last_name_node_8, c_preferred_cust_flag_node_8, c_birth_day_node_8, c_birth_month_node_8, c_birth_year_node_8, c_birth_country_node_8, c_login_node_8, c_email_address_node_8, c_last_review_date_node_8])
            +- HashJoin(joinType=[InnerJoin], where=[(c_current_addr_sk_node_80 = inv_date_sk_node_6)], select=[inv_date_sk_node_6, hd_demo_sk_node_7, hd_income_band_sk_node_7, hd_buy_potential_node_7, hd_dep_count_node_7, hd_vehicle_count_node_7, c_customer_sk_node_8, c_customer_id_node_8, c_current_cdemo_sk_node_8, c_current_hdemo_sk_node_8, c_current_addr_sk_node_8, c_first_shipto_date_sk_node_8, c_first_sales_date_sk_node_8, c_salutation_node_8, c_first_name_node_8, c_last_name_node_8, c_preferred_cust_flag_node_8, c_birth_day_node_8, c_birth_month_node_8, c_birth_year_node_8, c_birth_country_node_8, c_login_node_8, c_email_address_node_8, c_last_review_date_node_8, c_current_addr_sk_node_80], isBroadcast=[true], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- HashAggregate(isMerge=[true], groupBy=[inv_date_sk_node_6], select=[inv_date_sk_node_6])
               :     +- Exchange(distribution=[hash[inv_date_sk_node_6]])
               :        +- LocalHashAggregate(groupBy=[inv_date_sk_node_6], select=[inv_date_sk_node_6])
               :           +- Calc(select=[EXPR$0 AS inv_date_sk_node_6])
               :              +- HashAggregate(isMerge=[true], groupBy=[inv_date_sk], select=[inv_date_sk, Final_COUNT(count$0) AS EXPR$0])
               :                 +- Exchange(distribution=[hash[inv_date_sk]])
               :                    +- LocalHashAggregate(groupBy=[inv_date_sk], select=[inv_date_sk, Partial_COUNT(inv_date_sk) AS count$0])
               :                       +- TableSourceScan(table=[[default_catalog, default_database, inventory, project=[inv_date_sk], metadata=[]]], fields=[inv_date_sk])
               +- Calc(select=[hd_demo_sk AS hd_demo_sk_node_7, hd_income_band_sk AS hd_income_band_sk_node_7, hd_buy_potential AS hd_buy_potential_node_7, hd_dep_count AS hd_dep_count_node_7, hd_vehicle_count AS hd_vehicle_count_node_7, c_customer_sk AS c_customer_sk_node_8, c_customer_id AS c_customer_id_node_8, c_current_cdemo_sk AS c_current_cdemo_sk_node_8, c_current_hdemo_sk AS c_current_hdemo_sk_node_8, c_current_addr_sk AS c_current_addr_sk_node_8, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_8, c_first_sales_date_sk AS c_first_sales_date_sk_node_8, c_salutation AS c_salutation_node_8, c_first_name AS c_first_name_node_8, c_last_name AS c_last_name_node_8, c_preferred_cust_flag AS c_preferred_cust_flag_node_8, c_birth_day AS c_birth_day_node_8, c_birth_month AS c_birth_month_node_8, c_birth_year AS c_birth_year_node_8, c_birth_country AS c_birth_country_node_8, c_login AS c_login_node_8, c_email_address AS c_email_address_node_8, c_last_review_date AS c_last_review_date_node_8, CAST(c_current_addr_sk AS BIGINT) AS c_current_addr_sk_node_80])
                  +- Sort(orderBy=[c_birth_year ASC])
                     +- Exchange(distribution=[single])
                        +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(c_current_addr_sk = hd_income_band_sk)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])\
])
                           :- Exchange(distribution=[broadcast])
                           :  +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                           +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o137335594.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#277379615:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[18](input=RelSubset#277379613,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[18]), rel#277379612:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[18](input=RelSubset#277379611,groupBy=c_current_hdemo_sk_node_8, c_current_addr_sk_node_80,select=c_current_hdemo_sk_node_8, c_current_addr_sk_node_80, Partial_MIN(c_customer_sk_node_8) AS min$0)]
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