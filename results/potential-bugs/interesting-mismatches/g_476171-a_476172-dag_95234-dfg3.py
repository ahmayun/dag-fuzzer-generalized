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

autonode_10 = table_env.from_path("reason").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("reason").get_schema().get_field_names()])
autonode_12 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_11 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_7 = autonode_10.limit(59)
autonode_9 = autonode_12.order_by(col('cd_dep_employed_count_node_12'))
autonode_8 = autonode_11.order_by(col('wr_account_credit_node_11'))
autonode_6 = autonode_9.alias('SoLpd')
autonode_5 = autonode_7.join(autonode_8, col('wr_return_quantity_node_11') == col('r_reason_sk_node_10'))
autonode_4 = autonode_6.limit(83)
autonode_3 = autonode_5.distinct()
autonode_2 = autonode_3.join(autonode_4, col('cd_gender_node_12') == col('r_reason_desc_node_10'))
autonode_1 = autonode_2.group_by(col('wr_item_sk_node_11')).select(col('wr_account_credit_node_11').count.alias('wr_account_credit_node_11'))
sink = autonode_1.select(col('wr_account_credit_node_11'))
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
LogicalProject(wr_account_credit_node_11=[$1])
+- LogicalAggregate(group=[{5}], EXPR$0=[COUNT($25)])
   +- LogicalJoin(condition=[=($28, $2)], joinType=[inner])
      :- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26}])
      :  +- LogicalJoin(condition=[=($17, $0)], joinType=[inner])
      :     :- LogicalSort(fetch=[59])
      :     :  +- LogicalProject(r_reason_sk_node_10=[$0], r_reason_id_node_10=[$1], r_reason_desc_node_10=[$2])
      :     :     +- LogicalTableScan(table=[[default_catalog, default_database, reason]])
      :     +- LogicalSort(sort0=[$22], dir0=[ASC])
      :        +- LogicalProject(wr_returned_date_sk_node_11=[$0], wr_returned_time_sk_node_11=[$1], wr_item_sk_node_11=[$2], wr_refunded_customer_sk_node_11=[$3], wr_refunded_cdemo_sk_node_11=[$4], wr_refunded_hdemo_sk_node_11=[$5], wr_refunded_addr_sk_node_11=[$6], wr_returning_customer_sk_node_11=[$7], wr_returning_cdemo_sk_node_11=[$8], wr_returning_hdemo_sk_node_11=[$9], wr_returning_addr_sk_node_11=[$10], wr_web_page_sk_node_11=[$11], wr_reason_sk_node_11=[$12], wr_order_number_node_11=[$13], wr_return_quantity_node_11=[$14], wr_return_amt_node_11=[$15], wr_return_tax_node_11=[$16], wr_return_amt_inc_tax_node_11=[$17], wr_fee_node_11=[$18], wr_return_ship_cost_node_11=[$19], wr_refunded_cash_node_11=[$20], wr_reversed_charge_node_11=[$21], wr_account_credit_node_11=[$22], wr_net_loss_node_11=[$23])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
      +- LogicalProject(SoLpd=[AS($0, _UTF-16LE'SoLpd')], cd_gender_node_12=[$1], cd_marital_status_node_12=[$2], cd_education_status_node_12=[$3], cd_purchase_estimate_node_12=[$4], cd_credit_rating_node_12=[$5], cd_dep_count_node_12=[$6], cd_dep_employed_count_node_12=[$7], cd_dep_college_count_node_12=[$8])
         +- LogicalSort(sort0=[$7], dir0=[ASC], fetch=[83])
            +- LogicalProject(cd_demo_sk_node_12=[$0], cd_gender_node_12=[$1], cd_marital_status_node_12=[$2], cd_education_status_node_12=[$3], cd_purchase_estimate_node_12=[$4], cd_credit_rating_node_12=[$5], cd_dep_count_node_12=[$6], cd_dep_employed_count_node_12=[$7], cd_dep_college_count_node_12=[$8])
               +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS wr_account_credit_node_11])
+- HashAggregate(isMerge=[true], groupBy=[wr_item_sk], select=[wr_item_sk, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[hash[wr_item_sk]])
      +- LocalHashAggregate(groupBy=[wr_item_sk], select=[wr_item_sk, Partial_COUNT(wr_account_credit) AS count$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(cd_gender_node_12, r_reason_desc)], select=[r_reason_sk, r_reason_id, r_reason_desc, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, SoLpd, cd_gender_node_12, cd_marital_status_node_12, cd_education_status_node_12, cd_purchase_estimate_node_12, cd_credit_rating_node_12, cd_dep_count_node_12, cd_dep_employed_count_node_12, cd_dep_college_count_node_12], isBroadcast=[true], build=[right])
            :- HashAggregate(isMerge=[false], groupBy=[r_reason_sk, r_reason_id, r_reason_desc, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], select=[r_reason_sk, r_reason_id, r_reason_desc, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
            :  +- Exchange(distribution=[hash[r_reason_sk, r_reason_id, r_reason_desc, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss]])
            :     +- HashJoin(joinType=[InnerJoin], where=[=(wr_return_quantity, r_reason_sk)], select=[r_reason_sk, r_reason_id, r_reason_desc, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
            :        :- Exchange(distribution=[broadcast])
            :        :  +- Limit(offset=[0], fetch=[59], global=[true])
            :        :     +- Exchange(distribution=[single])
            :        :        +- Limit(offset=[0], fetch=[59], global=[false])
            :        :           +- TableSourceScan(table=[[default_catalog, default_database, reason]], fields=[r_reason_sk, r_reason_id, r_reason_desc])
            :        +- Sort(orderBy=[wr_account_credit ASC])
            :           +- Exchange(distribution=[single])
            :              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[cd_demo_sk AS SoLpd, cd_gender AS cd_gender_node_12, cd_marital_status AS cd_marital_status_node_12, cd_education_status AS cd_education_status_node_12, cd_purchase_estimate AS cd_purchase_estimate_node_12, cd_credit_rating AS cd_credit_rating_node_12, cd_dep_count AS cd_dep_count_node_12, cd_dep_employed_count AS cd_dep_employed_count_node_12, cd_dep_college_count AS cd_dep_college_count_node_12])
                  +- SortLimit(orderBy=[cd_dep_employed_count ASC], offset=[0], fetch=[83], global=[true])
                     +- Exchange(distribution=[single])
                        +- SortLimit(orderBy=[cd_dep_employed_count ASC], offset=[0], fetch=[83], global=[false])
                           +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS wr_account_credit_node_11])
+- HashAggregate(isMerge=[true], groupBy=[wr_item_sk], select=[wr_item_sk, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[hash[wr_item_sk]])
      +- LocalHashAggregate(groupBy=[wr_item_sk], select=[wr_item_sk, Partial_COUNT(wr_account_credit) AS count$0])
         +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(cd_gender_node_12 = r_reason_desc)], select=[r_reason_sk, r_reason_id, r_reason_desc, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, SoLpd, cd_gender_node_12, cd_marital_status_node_12, cd_education_status_node_12, cd_purchase_estimate_node_12, cd_credit_rating_node_12, cd_dep_count_node_12, cd_dep_employed_count_node_12, cd_dep_college_count_node_12], isBroadcast=[true], build=[right])\
:- HashAggregate(isMerge=[false], groupBy=[r_reason_sk, r_reason_id, r_reason_desc, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], select=[r_reason_sk, r_reason_id, r_reason_desc, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])\
:  +- [#2] Exchange(distribution=[hash[r_reason_sk, r_reason_id, r_reason_desc, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss]])\
+- [#1] Exchange(distribution=[broadcast])\
])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[cd_demo_sk AS SoLpd, cd_gender AS cd_gender_node_12, cd_marital_status AS cd_marital_status_node_12, cd_education_status AS cd_education_status_node_12, cd_purchase_estimate AS cd_purchase_estimate_node_12, cd_credit_rating AS cd_credit_rating_node_12, cd_dep_count AS cd_dep_count_node_12, cd_dep_employed_count AS cd_dep_employed_count_node_12, cd_dep_college_count AS cd_dep_college_count_node_12])
            :     +- SortLimit(orderBy=[cd_dep_employed_count ASC], offset=[0], fetch=[83], global=[true])
            :        +- Exchange(distribution=[single])
            :           +- SortLimit(orderBy=[cd_dep_employed_count ASC], offset=[0], fetch=[83], global=[false])
            :              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
            +- Exchange(distribution=[hash[r_reason_sk, r_reason_id, r_reason_desc, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss]])
               +- HashJoin(joinType=[InnerJoin], where=[(wr_return_quantity = r_reason_sk)], select=[r_reason_sk, r_reason_id, r_reason_desc, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Limit(offset=[0], fetch=[59], global=[true])
                  :     +- Exchange(distribution=[single])
                  :        +- Limit(offset=[0], fetch=[59], global=[false])
                  :           +- TableSourceScan(table=[[default_catalog, default_database, reason]], fields=[r_reason_sk, r_reason_id, r_reason_desc])
                  +- Sort(orderBy=[wr_account_credit ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o259294375.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#524299384:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[7](input=RelSubset#524299382,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[7]), rel#524299381:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[7](input=RelSubset#524299380,groupBy=cd_gender,select=cd_gender, Partial_COUNT(*) AS count1$0)]
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