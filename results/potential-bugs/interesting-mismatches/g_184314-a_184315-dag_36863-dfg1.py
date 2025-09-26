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
    import numpy as np
    return np.exp(np.log(values[values > 0]).mean()) if (values > 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_9 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_11 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_7 = autonode_9.alias('0jryi')
autonode_8 = autonode_10.join(autonode_11, col('p_end_date_sk_node_11') == col('ss_sold_date_sk_node_10'))
autonode_5 = autonode_7.alias('vs0iV')
autonode_6 = autonode_8.order_by(col('ss_ext_discount_amt_node_10'))
autonode_4 = autonode_5.join(autonode_6, col('cp_department_node_9') == col('p_promo_name_node_11'))
autonode_3 = autonode_4.group_by(col('p_channel_demo_node_11')).select(col('ss_ext_wholesale_cost_node_10').count.alias('ss_ext_wholesale_cost_node_10'))
autonode_2 = autonode_3.order_by(col('ss_ext_wholesale_cost_node_10'))
autonode_1 = autonode_2.alias('Aa13E')
sink = autonode_1.limit(4)
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
LogicalProject(Aa13E=[AS($0, _UTF-16LE'Aa13E')])
+- LogicalSort(sort0=[$0], dir0=[ASC], fetch=[4])
   +- LogicalProject(ss_ext_wholesale_cost_node_10=[$1])
      +- LogicalAggregate(group=[{47}], EXPR$0=[COUNT($25)])
         +- LogicalJoin(condition=[=($4, $39)], joinType=[inner])
            :- LogicalProject(vs0iV=[AS(AS($0, _UTF-16LE'0jryi'), _UTF-16LE'vs0iV')], cp_catalog_page_id_node_9=[$1], cp_start_date_sk_node_9=[$2], cp_end_date_sk_node_9=[$3], cp_department_node_9=[$4], cp_catalog_number_node_9=[$5], cp_catalog_page_number_node_9=[$6], cp_description_node_9=[$7], cp_type_node_9=[$8])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])
            +- LogicalSort(sort0=[$14], dir0=[ASC])
               +- LogicalJoin(condition=[=($26, $0)], joinType=[inner])
                  :- LogicalProject(ss_sold_date_sk_node_10=[$0], ss_sold_time_sk_node_10=[$1], ss_item_sk_node_10=[$2], ss_customer_sk_node_10=[$3], ss_cdemo_sk_node_10=[$4], ss_hdemo_sk_node_10=[$5], ss_addr_sk_node_10=[$6], ss_store_sk_node_10=[$7], ss_promo_sk_node_10=[$8], ss_ticket_number_node_10=[$9], ss_quantity_node_10=[$10], ss_wholesale_cost_node_10=[$11], ss_list_price_node_10=[$12], ss_sales_price_node_10=[$13], ss_ext_discount_amt_node_10=[$14], ss_ext_sales_price_node_10=[$15], ss_ext_wholesale_cost_node_10=[$16], ss_ext_list_price_node_10=[$17], ss_ext_tax_node_10=[$18], ss_coupon_amt_node_10=[$19], ss_net_paid_node_10=[$20], ss_net_paid_inc_tax_node_10=[$21], ss_net_profit_node_10=[$22])
                  :  +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
                  +- LogicalProject(p_promo_sk_node_11=[$0], p_promo_id_node_11=[$1], p_start_date_sk_node_11=[$2], p_end_date_sk_node_11=[$3], p_item_sk_node_11=[$4], p_cost_node_11=[$5], p_response_target_node_11=[$6], p_promo_name_node_11=[$7], p_channel_dmail_node_11=[$8], p_channel_email_node_11=[$9], p_channel_catalog_node_11=[$10], p_channel_tv_node_11=[$11], p_channel_radio_node_11=[$12], p_channel_press_node_11=[$13], p_channel_event_node_11=[$14], p_channel_demo_node_11=[$15], p_channel_details_node_11=[$16], p_purpose_node_11=[$17], p_discount_active_node_11=[$18])
                     +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS Aa13E])
+- SortLimit(orderBy=[EXPR$0 ASC], offset=[0], fetch=[4], global=[true])
   +- Exchange(distribution=[single])
      +- SortLimit(orderBy=[EXPR$0 ASC], offset=[0], fetch=[4], global=[false])
         +- HashAggregate(isMerge=[false], groupBy=[p_channel_demo], select=[p_channel_demo, COUNT(ss_ext_wholesale_cost) AS EXPR$0])
            +- Exchange(distribution=[hash[p_channel_demo]])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cp_department_node_9, p_promo_name)], select=[vs0iV, cp_catalog_page_id_node_9, cp_start_date_sk_node_9, cp_end_date_sk_node_9, cp_department_node_9, cp_catalog_number_node_9, cp_catalog_page_number_node_9, cp_description_node_9, cp_type_node_9, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[cp_catalog_page_sk AS vs0iV, cp_catalog_page_id AS cp_catalog_page_id_node_9, cp_start_date_sk AS cp_start_date_sk_node_9, cp_end_date_sk AS cp_end_date_sk_node_9, cp_department AS cp_department_node_9, cp_catalog_number AS cp_catalog_number_node_9, cp_catalog_page_number AS cp_catalog_page_number_node_9, cp_description AS cp_description_node_9, cp_type AS cp_type_node_9])
                  :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
                  +- Sort(orderBy=[ss_ext_discount_amt ASC])
                     +- Exchange(distribution=[single])
                        +- HashJoin(joinType=[InnerJoin], where=[=(p_end_date_sk, ss_sold_date_sk)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], isBroadcast=[true], build=[right])
                           :- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                           +- Exchange(distribution=[broadcast])
                              +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS Aa13E])
+- SortLimit(orderBy=[EXPR$0 ASC], offset=[0], fetch=[4], global=[true])
   +- Exchange(distribution=[single])
      +- SortLimit(orderBy=[EXPR$0 ASC], offset=[0], fetch=[4], global=[false])
         +- HashAggregate(isMerge=[false], groupBy=[p_channel_demo], select=[p_channel_demo, COUNT(ss_ext_wholesale_cost) AS EXPR$0])
            +- Exchange(distribution=[hash[p_channel_demo]])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[(cp_department_node_9 = p_promo_name)], select=[vs0iV, cp_catalog_page_id_node_9, cp_start_date_sk_node_9, cp_end_date_sk_node_9, cp_department_node_9, cp_catalog_number_node_9, cp_catalog_page_number_node_9, cp_description_node_9, cp_type_node_9, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[cp_catalog_page_sk AS vs0iV, cp_catalog_page_id AS cp_catalog_page_id_node_9, cp_start_date_sk AS cp_start_date_sk_node_9, cp_end_date_sk AS cp_end_date_sk_node_9, cp_department AS cp_department_node_9, cp_catalog_number AS cp_catalog_number_node_9, cp_catalog_page_number AS cp_catalog_page_number_node_9, cp_description AS cp_description_node_9, cp_type AS cp_type_node_9])
                  :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
                  +- Sort(orderBy=[ss_ext_discount_amt ASC])
                     +- Exchange(distribution=[single])
                        +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(p_end_date_sk = ss_sold_date_sk)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])\
+- [#2] Exchange(distribution=[broadcast])\
])
                           :- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                           +- Exchange(distribution=[broadcast])
                              +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o100491247.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#202632185:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[14](input=RelSubset#202632183,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[14]), rel#202632182:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[14](input=RelSubset#202632181,groupBy=p_promo_name, p_channel_demo,select=p_promo_name, p_channel_demo, Partial_COUNT(ss_ext_wholesale_cost) AS count$0)]
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