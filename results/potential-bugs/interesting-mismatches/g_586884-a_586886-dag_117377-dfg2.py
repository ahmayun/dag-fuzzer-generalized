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
    return values.max() - values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_9 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_8 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_7 = autonode_9.order_by(col('c_salutation_node_9'))
autonode_6 = autonode_8.filter(col('cp_end_date_sk_node_8') <= 15)
autonode_5 = autonode_7.order_by(col('c_first_shipto_date_sk_node_9'))
autonode_4 = autonode_6.alias('Hk78h')
autonode_3 = autonode_5.alias('ww1an')
autonode_2 = autonode_4.filter(col('cp_type_node_8').char_length <= 5)
autonode_1 = autonode_2.join(autonode_3, col('c_birth_month_node_9') == col('cp_catalog_page_number_node_8'))
sink = autonode_1.group_by(col('c_preferred_cust_flag_node_9')).select(col('c_first_shipto_date_sk_node_9').sum.alias('c_first_shipto_date_sk_node_9'))
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
LogicalProject(c_first_shipto_date_sk_node_9=[$1])
+- LogicalAggregate(group=[{19}], EXPR$0=[SUM($14)])
   +- LogicalJoin(condition=[=($21, $6)], joinType=[inner])
      :- LogicalFilter(condition=[<=(CHAR_LENGTH($8), 5)])
      :  +- LogicalProject(Hk78h=[AS($0, _UTF-16LE'Hk78h')], cp_catalog_page_id_node_8=[$1], cp_start_date_sk_node_8=[$2], cp_end_date_sk_node_8=[$3], cp_department_node_8=[$4], cp_catalog_number_node_8=[$5], cp_catalog_page_number_node_8=[$6], cp_description_node_8=[$7], cp_type_node_8=[$8])
      :     +- LogicalFilter(condition=[<=($3, 15)])
      :        +- LogicalProject(cp_catalog_page_sk_node_8=[$0], cp_catalog_page_id_node_8=[$1], cp_start_date_sk_node_8=[$2], cp_end_date_sk_node_8=[$3], cp_department_node_8=[$4], cp_catalog_number_node_8=[$5], cp_catalog_page_number_node_8=[$6], cp_description_node_8=[$7], cp_type_node_8=[$8])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])
      +- LogicalProject(ww1an=[AS($0, _UTF-16LE'ww1an')], c_customer_id_node_9=[$1], c_current_cdemo_sk_node_9=[$2], c_current_hdemo_sk_node_9=[$3], c_current_addr_sk_node_9=[$4], c_first_shipto_date_sk_node_9=[$5], c_first_sales_date_sk_node_9=[$6], c_salutation_node_9=[$7], c_first_name_node_9=[$8], c_last_name_node_9=[$9], c_preferred_cust_flag_node_9=[$10], c_birth_day_node_9=[$11], c_birth_month_node_9=[$12], c_birth_year_node_9=[$13], c_birth_country_node_9=[$14], c_login_node_9=[$15], c_email_address_node_9=[$16], c_last_review_date_node_9=[$17])
         +- LogicalSort(sort0=[$5], dir0=[ASC])
            +- LogicalSort(sort0=[$7], dir0=[ASC])
               +- LogicalProject(c_customer_sk_node_9=[$0], c_customer_id_node_9=[$1], c_current_cdemo_sk_node_9=[$2], c_current_hdemo_sk_node_9=[$3], c_current_addr_sk_node_9=[$4], c_first_shipto_date_sk_node_9=[$5], c_first_sales_date_sk_node_9=[$6], c_salutation_node_9=[$7], c_first_name_node_9=[$8], c_last_name_node_9=[$9], c_preferred_cust_flag_node_9=[$10], c_birth_day_node_9=[$11], c_birth_month_node_9=[$12], c_birth_year_node_9=[$13], c_birth_country_node_9=[$14], c_login_node_9=[$15], c_email_address_node_9=[$16], c_last_review_date_node_9=[$17])
                  +- LogicalTableScan(table=[[default_catalog, default_database, customer]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS c_first_shipto_date_sk_node_9])
+- HashAggregate(isMerge=[true], groupBy=[c_preferred_cust_flag_node_9], select=[c_preferred_cust_flag_node_9, Final_SUM(sum$0) AS EXPR$0])
   +- Exchange(distribution=[hash[c_preferred_cust_flag_node_9]])
      +- LocalHashAggregate(groupBy=[c_preferred_cust_flag_node_9], select=[c_preferred_cust_flag_node_9, Partial_SUM(c_first_shipto_date_sk_node_9) AS sum$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(c_birth_month_node_9, cp_catalog_page_number_node_8)], select=[Hk78h, cp_catalog_page_id_node_8, cp_start_date_sk_node_8, cp_end_date_sk_node_8, cp_department_node_8, cp_catalog_number_node_8, cp_catalog_page_number_node_8, cp_description_node_8, cp_type_node_8, ww1an, c_customer_id_node_9, c_current_cdemo_sk_node_9, c_current_hdemo_sk_node_9, c_current_addr_sk_node_9, c_first_shipto_date_sk_node_9, c_first_sales_date_sk_node_9, c_salutation_node_9, c_first_name_node_9, c_last_name_node_9, c_preferred_cust_flag_node_9, c_birth_day_node_9, c_birth_month_node_9, c_birth_year_node_9, c_birth_country_node_9, c_login_node_9, c_email_address_node_9, c_last_review_date_node_9], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[cp_catalog_page_sk AS Hk78h, cp_catalog_page_id AS cp_catalog_page_id_node_8, cp_start_date_sk AS cp_start_date_sk_node_8, cp_end_date_sk AS cp_end_date_sk_node_8, cp_department AS cp_department_node_8, cp_catalog_number AS cp_catalog_number_node_8, cp_catalog_page_number AS cp_catalog_page_number_node_8, cp_description AS cp_description_node_8, cp_type AS cp_type_node_8], where=[AND(<=(cp_end_date_sk, 15), <=(CHAR_LENGTH(cp_type), 5))])
            :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_page, filter=[and(<=(cp_end_date_sk, 15), <=(CHAR_LENGTH(cp_type), 5))]]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
            +- Calc(select=[c_customer_sk AS ww1an, c_customer_id AS c_customer_id_node_9, c_current_cdemo_sk AS c_current_cdemo_sk_node_9, c_current_hdemo_sk AS c_current_hdemo_sk_node_9, c_current_addr_sk AS c_current_addr_sk_node_9, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_9, c_first_sales_date_sk AS c_first_sales_date_sk_node_9, c_salutation AS c_salutation_node_9, c_first_name AS c_first_name_node_9, c_last_name AS c_last_name_node_9, c_preferred_cust_flag AS c_preferred_cust_flag_node_9, c_birth_day AS c_birth_day_node_9, c_birth_month AS c_birth_month_node_9, c_birth_year AS c_birth_year_node_9, c_birth_country AS c_birth_country_node_9, c_login AS c_login_node_9, c_email_address AS c_email_address_node_9, c_last_review_date AS c_last_review_date_node_9])
               +- Sort(orderBy=[c_first_shipto_date_sk ASC])
                  +- Sort(orderBy=[c_salutation ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS c_first_shipto_date_sk_node_9])
+- HashAggregate(isMerge=[true], groupBy=[c_preferred_cust_flag_node_9], select=[c_preferred_cust_flag_node_9, Final_SUM(sum$0) AS EXPR$0])
   +- Exchange(distribution=[hash[c_preferred_cust_flag_node_9]])
      +- LocalHashAggregate(groupBy=[c_preferred_cust_flag_node_9], select=[c_preferred_cust_flag_node_9, Partial_SUM(c_first_shipto_date_sk_node_9) AS sum$0])
         +- HashJoin(joinType=[InnerJoin], where=[(c_birth_month_node_9 = cp_catalog_page_number_node_8)], select=[Hk78h, cp_catalog_page_id_node_8, cp_start_date_sk_node_8, cp_end_date_sk_node_8, cp_department_node_8, cp_catalog_number_node_8, cp_catalog_page_number_node_8, cp_description_node_8, cp_type_node_8, ww1an, c_customer_id_node_9, c_current_cdemo_sk_node_9, c_current_hdemo_sk_node_9, c_current_addr_sk_node_9, c_first_shipto_date_sk_node_9, c_first_sales_date_sk_node_9, c_salutation_node_9, c_first_name_node_9, c_last_name_node_9, c_preferred_cust_flag_node_9, c_birth_day_node_9, c_birth_month_node_9, c_birth_year_node_9, c_birth_country_node_9, c_login_node_9, c_email_address_node_9, c_last_review_date_node_9], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[cp_catalog_page_sk AS Hk78h, cp_catalog_page_id AS cp_catalog_page_id_node_8, cp_start_date_sk AS cp_start_date_sk_node_8, cp_end_date_sk AS cp_end_date_sk_node_8, cp_department AS cp_department_node_8, cp_catalog_number AS cp_catalog_number_node_8, cp_catalog_page_number AS cp_catalog_page_number_node_8, cp_description AS cp_description_node_8, cp_type AS cp_type_node_8], where=[((cp_end_date_sk <= 15) AND (CHAR_LENGTH(cp_type) <= 5))])
            :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_page, filter=[and(<=(cp_end_date_sk, 15), <=(CHAR_LENGTH(cp_type), 5))]]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
            +- Calc(select=[c_customer_sk AS ww1an, c_customer_id AS c_customer_id_node_9, c_current_cdemo_sk AS c_current_cdemo_sk_node_9, c_current_hdemo_sk AS c_current_hdemo_sk_node_9, c_current_addr_sk AS c_current_addr_sk_node_9, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_9, c_first_sales_date_sk AS c_first_sales_date_sk_node_9, c_salutation AS c_salutation_node_9, c_first_name AS c_first_name_node_9, c_last_name AS c_last_name_node_9, c_preferred_cust_flag AS c_preferred_cust_flag_node_9, c_birth_day AS c_birth_day_node_9, c_birth_month AS c_birth_month_node_9, c_birth_year AS c_birth_year_node_9, c_birth_country AS c_birth_country_node_9, c_login AS c_login_node_9, c_email_address AS c_email_address_node_9, c_last_review_date AS c_last_review_date_node_9])
               +- Sort(orderBy=[c_first_shipto_date_sk ASC])
                  +- Sort(orderBy=[c_salutation ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o320104913.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#645388139:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[5](input=RelSubset#645388137,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[5]), rel#645388136:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[5](input=RelSubset#645388135,groupBy=c_preferred_cust_flag, c_birth_month,select=c_preferred_cust_flag, c_birth_month, Partial_SUM(c_first_shipto_date_sk) AS sum$0)]
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