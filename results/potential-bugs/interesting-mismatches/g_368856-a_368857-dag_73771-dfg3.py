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
    return values.count()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_9 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_8 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_7 = autonode_9.alias('U8QS0')
autonode_6 = autonode_8.filter(col('cd_credit_rating_node_8').char_length <= 5)
autonode_5 = autonode_7.order_by(col('wr_item_sk_node_9'))
autonode_4 = autonode_6.limit(31)
autonode_3 = autonode_4.join(autonode_5, col('wr_refunded_hdemo_sk_node_9') == col('cd_demo_sk_node_8'))
autonode_2 = autonode_3.group_by(col('wr_refunded_hdemo_sk_node_9')).select(col('wr_return_amt_node_9').count.alias('wr_return_amt_node_9'))
autonode_1 = autonode_2.distinct()
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
      "error_message": "An error occurred while calling o200683737.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#405925627:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[2](input=RelSubset#405925625,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[2]), rel#405925624:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[2](input=RelSubset#405925623,groupBy=wr_refunded_hdemo_sk,select=wr_refunded_hdemo_sk, Partial_COUNT(wr_return_amt) AS count$0)]
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
LogicalProject(wr_return_amt_node_9=[$0], _c1=[_UTF-16LE'hello'])
+- LogicalAggregate(group=[{0}])
   +- LogicalProject(wr_return_amt_node_9=[$1])
      +- LogicalAggregate(group=[{14}], EXPR$0=[COUNT($24)])
         +- LogicalJoin(condition=[=($14, $0)], joinType=[inner])
            :- LogicalSort(fetch=[31])
            :  +- LogicalFilter(condition=[<=(CHAR_LENGTH($5), 5)])
            :     +- LogicalProject(cd_demo_sk_node_8=[$0], cd_gender_node_8=[$1], cd_marital_status_node_8=[$2], cd_education_status_node_8=[$3], cd_purchase_estimate_node_8=[$4], cd_credit_rating_node_8=[$5], cd_dep_count_node_8=[$6], cd_dep_employed_count_node_8=[$7], cd_dep_college_count_node_8=[$8])
            :        +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
            +- LogicalSort(sort0=[$2], dir0=[ASC])
               +- LogicalProject(U8QS0=[AS($0, _UTF-16LE'U8QS0')], wr_returned_time_sk_node_9=[$1], wr_item_sk_node_9=[$2], wr_refunded_customer_sk_node_9=[$3], wr_refunded_cdemo_sk_node_9=[$4], wr_refunded_hdemo_sk_node_9=[$5], wr_refunded_addr_sk_node_9=[$6], wr_returning_customer_sk_node_9=[$7], wr_returning_cdemo_sk_node_9=[$8], wr_returning_hdemo_sk_node_9=[$9], wr_returning_addr_sk_node_9=[$10], wr_web_page_sk_node_9=[$11], wr_reason_sk_node_9=[$12], wr_order_number_node_9=[$13], wr_return_quantity_node_9=[$14], wr_return_amt_node_9=[$15], wr_return_tax_node_9=[$16], wr_return_amt_inc_tax_node_9=[$17], wr_fee_node_9=[$18], wr_return_ship_cost_node_9=[$19], wr_refunded_cash_node_9=[$20], wr_reversed_charge_node_9=[$21], wr_account_credit_node_9=[$22], wr_net_loss_node_9=[$23])
                  +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])

== Optimized Physical Plan ==
Calc(select=[wr_return_amt_node_9, 'hello' AS _c1])
+- SortAggregate(isMerge=[true], groupBy=[wr_return_amt_node_9], select=[wr_return_amt_node_9])
   +- Sort(orderBy=[wr_return_amt_node_9 ASC])
      +- Exchange(distribution=[hash[wr_return_amt_node_9]])
         +- LocalSortAggregate(groupBy=[wr_return_amt_node_9], select=[wr_return_amt_node_9])
            +- Sort(orderBy=[wr_return_amt_node_9 ASC])
               +- Calc(select=[EXPR$0 AS wr_return_amt_node_9])
                  +- SortAggregate(isMerge=[true], groupBy=[wr_refunded_hdemo_sk_node_9], select=[wr_refunded_hdemo_sk_node_9, Final_COUNT(count$0) AS EXPR$0])
                     +- Sort(orderBy=[wr_refunded_hdemo_sk_node_9 ASC])
                        +- Exchange(distribution=[hash[wr_refunded_hdemo_sk_node_9]])
                           +- LocalSortAggregate(groupBy=[wr_refunded_hdemo_sk_node_9], select=[wr_refunded_hdemo_sk_node_9, Partial_COUNT(wr_return_amt_node_9) AS count$0])
                              +- Sort(orderBy=[wr_refunded_hdemo_sk_node_9 ASC])
                                 +- NestedLoopJoin(joinType=[InnerJoin], where=[=(wr_refunded_hdemo_sk_node_9, cd_demo_sk)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, U8QS0, wr_returned_time_sk_node_9, wr_item_sk_node_9, wr_refunded_customer_sk_node_9, wr_refunded_cdemo_sk_node_9, wr_refunded_hdemo_sk_node_9, wr_refunded_addr_sk_node_9, wr_returning_customer_sk_node_9, wr_returning_cdemo_sk_node_9, wr_returning_hdemo_sk_node_9, wr_returning_addr_sk_node_9, wr_web_page_sk_node_9, wr_reason_sk_node_9, wr_order_number_node_9, wr_return_quantity_node_9, wr_return_amt_node_9, wr_return_tax_node_9, wr_return_amt_inc_tax_node_9, wr_fee_node_9, wr_return_ship_cost_node_9, wr_refunded_cash_node_9, wr_reversed_charge_node_9, wr_account_credit_node_9, wr_net_loss_node_9], build=[right])
                                    :- Limit(offset=[0], fetch=[31], global=[true])
                                    :  +- Exchange(distribution=[single])
                                    :     +- Limit(offset=[0], fetch=[31], global=[false])
                                    :        +- Calc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[<=(CHAR_LENGTH(cd_credit_rating), 5)])
                                    :           +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[<=(CHAR_LENGTH(cd_credit_rating), 5)]]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                                    +- Exchange(distribution=[broadcast])
                                       +- Calc(select=[wr_returned_date_sk AS U8QS0, wr_returned_time_sk AS wr_returned_time_sk_node_9, wr_item_sk AS wr_item_sk_node_9, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_9, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_9, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_9, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_9, wr_returning_customer_sk AS wr_returning_customer_sk_node_9, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_9, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_9, wr_returning_addr_sk AS wr_returning_addr_sk_node_9, wr_web_page_sk AS wr_web_page_sk_node_9, wr_reason_sk AS wr_reason_sk_node_9, wr_order_number AS wr_order_number_node_9, wr_return_quantity AS wr_return_quantity_node_9, wr_return_amt AS wr_return_amt_node_9, wr_return_tax AS wr_return_tax_node_9, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_9, wr_fee AS wr_fee_node_9, wr_return_ship_cost AS wr_return_ship_cost_node_9, wr_refunded_cash AS wr_refunded_cash_node_9, wr_reversed_charge AS wr_reversed_charge_node_9, wr_account_credit AS wr_account_credit_node_9, wr_net_loss AS wr_net_loss_node_9])
                                          +- SortLimit(orderBy=[wr_item_sk ASC], offset=[0], fetch=[1], global=[true])
                                             +- Exchange(distribution=[single])
                                                +- SortLimit(orderBy=[wr_item_sk ASC], offset=[0], fetch=[1], global=[false])
                                                   +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])

== Optimized Execution Plan ==
Calc(select=[wr_return_amt_node_9, 'hello' AS _c1])
+- SortAggregate(isMerge=[true], groupBy=[wr_return_amt_node_9], select=[wr_return_amt_node_9])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[wr_return_amt_node_9 ASC])
         +- Exchange(distribution=[hash[wr_return_amt_node_9]])
            +- LocalSortAggregate(groupBy=[wr_return_amt_node_9], select=[wr_return_amt_node_9])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[wr_return_amt_node_9 ASC])
                     +- Calc(select=[EXPR$0 AS wr_return_amt_node_9])
                        +- SortAggregate(isMerge=[true], groupBy=[wr_refunded_hdemo_sk_node_9], select=[wr_refunded_hdemo_sk_node_9, Final_COUNT(count$0) AS EXPR$0])
                           +- Exchange(distribution=[forward])
                              +- Sort(orderBy=[wr_refunded_hdemo_sk_node_9 ASC])
                                 +- Exchange(distribution=[hash[wr_refunded_hdemo_sk_node_9]])
                                    +- LocalSortAggregate(groupBy=[wr_refunded_hdemo_sk_node_9], select=[wr_refunded_hdemo_sk_node_9, Partial_COUNT(wr_return_amt_node_9) AS count$0])
                                       +- Exchange(distribution=[forward])
                                          +- Sort(orderBy=[wr_refunded_hdemo_sk_node_9 ASC])
                                             +- NestedLoopJoin(joinType=[InnerJoin], where=[(wr_refunded_hdemo_sk_node_9 = cd_demo_sk)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, U8QS0, wr_returned_time_sk_node_9, wr_item_sk_node_9, wr_refunded_customer_sk_node_9, wr_refunded_cdemo_sk_node_9, wr_refunded_hdemo_sk_node_9, wr_refunded_addr_sk_node_9, wr_returning_customer_sk_node_9, wr_returning_cdemo_sk_node_9, wr_returning_hdemo_sk_node_9, wr_returning_addr_sk_node_9, wr_web_page_sk_node_9, wr_reason_sk_node_9, wr_order_number_node_9, wr_return_quantity_node_9, wr_return_amt_node_9, wr_return_tax_node_9, wr_return_amt_inc_tax_node_9, wr_fee_node_9, wr_return_ship_cost_node_9, wr_refunded_cash_node_9, wr_reversed_charge_node_9, wr_account_credit_node_9, wr_net_loss_node_9], build=[right])
                                                :- Limit(offset=[0], fetch=[31], global=[true])
                                                :  +- Exchange(distribution=[single])
                                                :     +- Limit(offset=[0], fetch=[31], global=[false])
                                                :        +- Calc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[(CHAR_LENGTH(cd_credit_rating) <= 5)])
                                                :           +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[<=(CHAR_LENGTH(cd_credit_rating), 5)]]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                                                +- Exchange(distribution=[broadcast])
                                                   +- Calc(select=[wr_returned_date_sk AS U8QS0, wr_returned_time_sk AS wr_returned_time_sk_node_9, wr_item_sk AS wr_item_sk_node_9, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_9, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_9, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_9, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_9, wr_returning_customer_sk AS wr_returning_customer_sk_node_9, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_9, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_9, wr_returning_addr_sk AS wr_returning_addr_sk_node_9, wr_web_page_sk AS wr_web_page_sk_node_9, wr_reason_sk AS wr_reason_sk_node_9, wr_order_number AS wr_order_number_node_9, wr_return_quantity AS wr_return_quantity_node_9, wr_return_amt AS wr_return_amt_node_9, wr_return_tax AS wr_return_tax_node_9, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_9, wr_fee AS wr_fee_node_9, wr_return_ship_cost AS wr_return_ship_cost_node_9, wr_refunded_cash AS wr_refunded_cash_node_9, wr_reversed_charge AS wr_reversed_charge_node_9, wr_account_credit AS wr_account_credit_node_9, wr_net_loss AS wr_net_loss_node_9])
                                                      +- SortLimit(orderBy=[wr_item_sk ASC], offset=[0], fetch=[1], global=[true])
                                                         +- Exchange(distribution=[single])
                                                            +- SortLimit(orderBy=[wr_item_sk ASC], offset=[0], fetch=[1], global=[false])
                                                               +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0