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
    return values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_14 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_15 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_10 = autonode_13.select(col('i_class_id_node_13'))
autonode_11 = autonode_14.add_columns(lit("hello"))
autonode_12 = autonode_15.distinct()
autonode_7 = autonode_10.group_by(col('i_class_id_node_13')).select(col('i_class_id_node_13').max.alias('i_class_id_node_13'))
autonode_8 = autonode_11.order_by(col('c_last_review_date_node_14'))
autonode_9 = autonode_12.select(col('ca_state_node_15'))
autonode_5 = autonode_7.order_by(col('i_class_id_node_13'))
autonode_6 = autonode_8.join(autonode_9, col('c_last_review_date_node_14') == col('ca_state_node_15'))
autonode_3 = autonode_5.limit(79)
autonode_4 = autonode_6.group_by(col('ca_state_node_15')).select(col('c_current_addr_sk_node_14').max.alias('c_current_addr_sk_node_14'))
autonode_1 = autonode_3.group_by(col('i_class_id_node_13')).select(col('i_class_id_node_13').sum.alias('i_class_id_node_13'))
autonode_2 = autonode_4.order_by(col('c_current_addr_sk_node_14'))
sink = autonode_1.join(autonode_2, col('i_class_id_node_13') == col('c_current_addr_sk_node_14'))
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
LogicalJoin(condition=[=($0, $1)], joinType=[inner])
:- LogicalProject(i_class_id_node_13=[$1])
:  +- LogicalAggregate(group=[{0}], EXPR$0=[SUM($0)])
:     +- LogicalSort(sort0=[$0], dir0=[ASC], fetch=[79])
:        +- LogicalProject(i_class_id_node_13=[$1])
:           +- LogicalAggregate(group=[{0}], EXPR$0=[MAX($0)])
:              +- LogicalProject(i_class_id_node_13=[$9])
:                 +- LogicalTableScan(table=[[default_catalog, default_database, item]])
+- LogicalSort(sort0=[$0], dir0=[ASC])
   +- LogicalProject(c_current_addr_sk_node_14=[$1])
      +- LogicalAggregate(group=[{19}], EXPR$0=[MAX($4)])
         +- LogicalJoin(condition=[=($17, $19)], joinType=[inner])
            :- LogicalSort(sort0=[$17], dir0=[ASC])
            :  +- LogicalProject(c_customer_sk_node_14=[$0], c_customer_id_node_14=[$1], c_current_cdemo_sk_node_14=[$2], c_current_hdemo_sk_node_14=[$3], c_current_addr_sk_node_14=[$4], c_first_shipto_date_sk_node_14=[$5], c_first_sales_date_sk_node_14=[$6], c_salutation_node_14=[$7], c_first_name_node_14=[$8], c_last_name_node_14=[$9], c_preferred_cust_flag_node_14=[$10], c_birth_day_node_14=[$11], c_birth_month_node_14=[$12], c_birth_year_node_14=[$13], c_birth_country_node_14=[$14], c_login_node_14=[$15], c_email_address_node_14=[$16], c_last_review_date_node_14=[$17], _c18=[_UTF-16LE'hello'])
            :     +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
            +- LogicalProject(ca_state_node_15=[$8])
               +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}])
                  +- LogicalProject(ca_address_sk_node_15=[$0], ca_address_id_node_15=[$1], ca_street_number_node_15=[$2], ca_street_name_node_15=[$3], ca_street_type_node_15=[$4], ca_suite_number_node_15=[$5], ca_city_node_15=[$6], ca_county_node_15=[$7], ca_state_node_15=[$8], ca_zip_node_15=[$9], ca_country_node_15=[$10], ca_gmt_offset_node_15=[$11], ca_location_type_node_15=[$12])
                     +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])

== Optimized Physical Plan ==
NestedLoopJoin(joinType=[InnerJoin], where=[=(i_class_id_node_13, c_current_addr_sk_node_14)], select=[i_class_id_node_13, c_current_addr_sk_node_14], build=[left])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[EXPR$0 AS i_class_id_node_13])
:     +- HashAggregate(isMerge=[true], groupBy=[i_class_id_node_13], select=[i_class_id_node_13, Final_SUM(sum$0) AS EXPR$0])
:        +- Sort(orderBy=[i_class_id_node_13 ASC])
:           +- Exchange(distribution=[hash[i_class_id_node_13]])
:              +- LocalHashAggregate(groupBy=[i_class_id_node_13], select=[i_class_id_node_13, Partial_SUM(i_class_id_node_13) AS sum$0])
:                 +- Calc(select=[EXPR$0 AS i_class_id_node_13])
:                    +- SortLimit(orderBy=[EXPR$0 ASC], offset=[0], fetch=[79], global=[true])
:                       +- Exchange(distribution=[single])
:                          +- SortLimit(orderBy=[EXPR$0 ASC], offset=[0], fetch=[79], global=[false])
:                             +- HashAggregate(isMerge=[true], groupBy=[i_class_id], select=[i_class_id, Final_MAX(max$0) AS EXPR$0])
:                                +- Exchange(distribution=[hash[i_class_id]])
:                                   +- LocalHashAggregate(groupBy=[i_class_id], select=[i_class_id, Partial_MAX(i_class_id) AS max$0])
:                                      +- TableSourceScan(table=[[default_catalog, default_database, item, project=[i_class_id], metadata=[]]], fields=[i_class_id])
+- Sort(orderBy=[c_current_addr_sk_node_14 ASC])
   +- Exchange(distribution=[single])
      +- Calc(select=[EXPR$0 AS c_current_addr_sk_node_14])
         +- HashAggregate(isMerge=[true], groupBy=[ca_state_node_15], select=[ca_state_node_15, Final_MAX(max$0) AS EXPR$0])
            +- Exchange(distribution=[hash[ca_state_node_15]])
               +- LocalHashAggregate(groupBy=[ca_state_node_15], select=[ca_state_node_15, Partial_MAX(c_current_addr_sk_node_14) AS max$0])
                  +- HashJoin(joinType=[InnerJoin], where=[=(c_last_review_date_node_14, ca_state_node_15)], select=[c_customer_sk_node_14, c_customer_id_node_14, c_current_cdemo_sk_node_14, c_current_hdemo_sk_node_14, c_current_addr_sk_node_14, c_first_shipto_date_sk_node_14, c_first_sales_date_sk_node_14, c_salutation_node_14, c_first_name_node_14, c_last_name_node_14, c_preferred_cust_flag_node_14, c_birth_day_node_14, c_birth_month_node_14, c_birth_year_node_14, c_birth_country_node_14, c_login_node_14, c_email_address_node_14, c_last_review_date_node_14, _c18, ca_state_node_15], isBroadcast=[true], build=[right])
                     :- Sort(orderBy=[c_last_review_date_node_14 ASC])
                     :  +- Calc(select=[c_customer_sk AS c_customer_sk_node_14, c_customer_id AS c_customer_id_node_14, c_current_cdemo_sk AS c_current_cdemo_sk_node_14, c_current_hdemo_sk AS c_current_hdemo_sk_node_14, c_current_addr_sk AS c_current_addr_sk_node_14, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_14, c_first_sales_date_sk AS c_first_sales_date_sk_node_14, c_salutation AS c_salutation_node_14, c_first_name AS c_first_name_node_14, c_last_name AS c_last_name_node_14, c_preferred_cust_flag AS c_preferred_cust_flag_node_14, c_birth_day AS c_birth_day_node_14, c_birth_month AS c_birth_month_node_14, c_birth_year AS c_birth_year_node_14, c_birth_country AS c_birth_country_node_14, c_login AS c_login_node_14, c_email_address AS c_email_address_node_14, c_last_review_date AS c_last_review_date_node_14, 'hello' AS _c18])
                     :     +- Exchange(distribution=[single])
                     :        +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                     +- Exchange(distribution=[broadcast])
                        +- Calc(select=[ca_state AS ca_state_node_15])
                           +- HashAggregate(isMerge=[false], groupBy=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                              +- Exchange(distribution=[hash[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type]])
                                 +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

== Optimized Execution Plan ==
NestedLoopJoin(joinType=[InnerJoin], where=[(i_class_id_node_13 = c_current_addr_sk_node_14)], select=[i_class_id_node_13, c_current_addr_sk_node_14], build=[left])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[EXPR$0 AS i_class_id_node_13])
:     +- HashAggregate(isMerge=[true], groupBy=[i_class_id_node_13], select=[i_class_id_node_13, Final_SUM(sum$0) AS EXPR$0])
:        +- Exchange(distribution=[keep_input_as_is[hash[i_class_id_node_13]]])
:           +- Sort(orderBy=[i_class_id_node_13 ASC])
:              +- Exchange(distribution=[hash[i_class_id_node_13]])
:                 +- LocalHashAggregate(groupBy=[i_class_id_node_13], select=[i_class_id_node_13, Partial_SUM(i_class_id_node_13) AS sum$0])
:                    +- Calc(select=[EXPR$0 AS i_class_id_node_13])
:                       +- SortLimit(orderBy=[EXPR$0 ASC], offset=[0], fetch=[79], global=[true])
:                          +- Exchange(distribution=[single])
:                             +- SortLimit(orderBy=[EXPR$0 ASC], offset=[0], fetch=[79], global=[false])
:                                +- HashAggregate(isMerge=[true], groupBy=[i_class_id], select=[i_class_id, Final_MAX(max$0) AS EXPR$0])
:                                   +- Exchange(distribution=[hash[i_class_id]])
:                                      +- LocalHashAggregate(groupBy=[i_class_id], select=[i_class_id, Partial_MAX(i_class_id) AS max$0])
:                                         +- TableSourceScan(table=[[default_catalog, default_database, item, project=[i_class_id], metadata=[]]], fields=[i_class_id])
+- Sort(orderBy=[c_current_addr_sk_node_14 ASC])
   +- Exchange(distribution=[single])
      +- Calc(select=[EXPR$0 AS c_current_addr_sk_node_14])
         +- HashAggregate(isMerge=[true], groupBy=[ca_state_node_15], select=[ca_state_node_15, Final_MAX(max$0) AS EXPR$0])
            +- Exchange(distribution=[hash[ca_state_node_15]])
               +- LocalHashAggregate(groupBy=[ca_state_node_15], select=[ca_state_node_15, Partial_MAX(c_current_addr_sk_node_14) AS max$0])
                  +- HashJoin(joinType=[InnerJoin], where=[(c_last_review_date_node_14 = ca_state_node_15)], select=[c_customer_sk_node_14, c_customer_id_node_14, c_current_cdemo_sk_node_14, c_current_hdemo_sk_node_14, c_current_addr_sk_node_14, c_first_shipto_date_sk_node_14, c_first_sales_date_sk_node_14, c_salutation_node_14, c_first_name_node_14, c_last_name_node_14, c_preferred_cust_flag_node_14, c_birth_day_node_14, c_birth_month_node_14, c_birth_year_node_14, c_birth_country_node_14, c_login_node_14, c_email_address_node_14, c_last_review_date_node_14, _c18, ca_state_node_15], isBroadcast=[true], build=[right])
                     :- Sort(orderBy=[c_last_review_date_node_14 ASC])
                     :  +- Calc(select=[c_customer_sk AS c_customer_sk_node_14, c_customer_id AS c_customer_id_node_14, c_current_cdemo_sk AS c_current_cdemo_sk_node_14, c_current_hdemo_sk AS c_current_hdemo_sk_node_14, c_current_addr_sk AS c_current_addr_sk_node_14, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_14, c_first_sales_date_sk AS c_first_sales_date_sk_node_14, c_salutation AS c_salutation_node_14, c_first_name AS c_first_name_node_14, c_last_name AS c_last_name_node_14, c_preferred_cust_flag AS c_preferred_cust_flag_node_14, c_birth_day AS c_birth_day_node_14, c_birth_month AS c_birth_month_node_14, c_birth_year AS c_birth_year_node_14, c_birth_country AS c_birth_country_node_14, c_login AS c_login_node_14, c_email_address AS c_email_address_node_14, c_last_review_date AS c_last_review_date_node_14, 'hello' AS _c18])
                     :     +- Exchange(distribution=[single])
                     :        +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                     +- Exchange(distribution=[broadcast])
                        +- Calc(select=[ca_state AS ca_state_node_15])
                           +- HashAggregate(isMerge=[false], groupBy=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                              +- Exchange(distribution=[hash[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type]])
                                 +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o27017460.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#53419221:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[17](input=RelSubset#53419219,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[17]), rel#53419218:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[17](input=RelSubset#53419217,groupBy=c_last_review_date,select=c_last_review_date, Partial_MAX(c_current_addr_sk) AS max$0)]
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