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
    return len(values) / (1.0 / values).sum() if (values != 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_9 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_8 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_7 = autonode_10.order_by(col('ss_coupon_amt_node_10'))
autonode_6 = autonode_8.join(autonode_9, col('wr_refunded_addr_sk_node_9') == col('cd_dep_count_node_8'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_4 = autonode_6.group_by(col('wr_return_tax_node_9')).select(col('wr_fee_node_9').avg.alias('wr_fee_node_9'))
autonode_3 = autonode_4.join(autonode_5, col('ss_net_paid_inc_tax_node_10') == col('wr_fee_node_9'))
autonode_2 = autonode_3.add_columns(lit("hello"))
autonode_1 = autonode_2.filter(col('ss_sold_time_sk_node_10') > 22)
sink = autonode_1.group_by(col('ss_net_profit_node_10')).select(col('ss_net_paid_inc_tax_node_10').max.alias('ss_net_paid_inc_tax_node_10'))
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
LogicalProject(ss_net_paid_inc_tax_node_10=[$1])
+- LogicalAggregate(group=[{23}], EXPR$0=[MAX($22)])
   +- LogicalFilter(condition=[>($2, 22)])
      +- LogicalProject(wr_fee_node_9=[$0], ss_sold_date_sk_node_10=[$1], ss_sold_time_sk_node_10=[$2], ss_item_sk_node_10=[$3], ss_customer_sk_node_10=[$4], ss_cdemo_sk_node_10=[$5], ss_hdemo_sk_node_10=[$6], ss_addr_sk_node_10=[$7], ss_store_sk_node_10=[$8], ss_promo_sk_node_10=[$9], ss_ticket_number_node_10=[$10], ss_quantity_node_10=[$11], ss_wholesale_cost_node_10=[$12], ss_list_price_node_10=[$13], ss_sales_price_node_10=[$14], ss_ext_discount_amt_node_10=[$15], ss_ext_sales_price_node_10=[$16], ss_ext_wholesale_cost_node_10=[$17], ss_ext_list_price_node_10=[$18], ss_ext_tax_node_10=[$19], ss_coupon_amt_node_10=[$20], ss_net_paid_node_10=[$21], ss_net_paid_inc_tax_node_10=[$22], ss_net_profit_node_10=[$23], _c23=[$24], _c25=[_UTF-16LE'hello'])
         +- LogicalJoin(condition=[=($22, $0)], joinType=[inner])
            :- LogicalProject(wr_fee_node_9=[$1])
            :  +- LogicalAggregate(group=[{25}], EXPR$0=[AVG($27)])
            :     +- LogicalJoin(condition=[=($15, $6)], joinType=[inner])
            :        :- LogicalProject(cd_demo_sk_node_8=[$0], cd_gender_node_8=[$1], cd_marital_status_node_8=[$2], cd_education_status_node_8=[$3], cd_purchase_estimate_node_8=[$4], cd_credit_rating_node_8=[$5], cd_dep_count_node_8=[$6], cd_dep_employed_count_node_8=[$7], cd_dep_college_count_node_8=[$8])
            :        :  +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
            :        +- LogicalProject(wr_returned_date_sk_node_9=[$0], wr_returned_time_sk_node_9=[$1], wr_item_sk_node_9=[$2], wr_refunded_customer_sk_node_9=[$3], wr_refunded_cdemo_sk_node_9=[$4], wr_refunded_hdemo_sk_node_9=[$5], wr_refunded_addr_sk_node_9=[$6], wr_returning_customer_sk_node_9=[$7], wr_returning_cdemo_sk_node_9=[$8], wr_returning_hdemo_sk_node_9=[$9], wr_returning_addr_sk_node_9=[$10], wr_web_page_sk_node_9=[$11], wr_reason_sk_node_9=[$12], wr_order_number_node_9=[$13], wr_return_quantity_node_9=[$14], wr_return_amt_node_9=[$15], wr_return_tax_node_9=[$16], wr_return_amt_inc_tax_node_9=[$17], wr_fee_node_9=[$18], wr_return_ship_cost_node_9=[$19], wr_refunded_cash_node_9=[$20], wr_reversed_charge_node_9=[$21], wr_account_credit_node_9=[$22], wr_net_loss_node_9=[$23])
            :           +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
            +- LogicalProject(ss_sold_date_sk_node_10=[$0], ss_sold_time_sk_node_10=[$1], ss_item_sk_node_10=[$2], ss_customer_sk_node_10=[$3], ss_cdemo_sk_node_10=[$4], ss_hdemo_sk_node_10=[$5], ss_addr_sk_node_10=[$6], ss_store_sk_node_10=[$7], ss_promo_sk_node_10=[$8], ss_ticket_number_node_10=[$9], ss_quantity_node_10=[$10], ss_wholesale_cost_node_10=[$11], ss_list_price_node_10=[$12], ss_sales_price_node_10=[$13], ss_ext_discount_amt_node_10=[$14], ss_ext_sales_price_node_10=[$15], ss_ext_wholesale_cost_node_10=[$16], ss_ext_list_price_node_10=[$17], ss_ext_tax_node_10=[$18], ss_coupon_amt_node_10=[$19], ss_net_paid_node_10=[$20], ss_net_paid_inc_tax_node_10=[$21], ss_net_profit_node_10=[$22], _c23=[_UTF-16LE'hello'])
               +- LogicalSort(sort0=[$19], dir0=[ASC])
                  +- LogicalProject(ss_sold_date_sk_node_10=[$0], ss_sold_time_sk_node_10=[$1], ss_item_sk_node_10=[$2], ss_customer_sk_node_10=[$3], ss_cdemo_sk_node_10=[$4], ss_hdemo_sk_node_10=[$5], ss_addr_sk_node_10=[$6], ss_store_sk_node_10=[$7], ss_promo_sk_node_10=[$8], ss_ticket_number_node_10=[$9], ss_quantity_node_10=[$10], ss_wholesale_cost_node_10=[$11], ss_list_price_node_10=[$12], ss_sales_price_node_10=[$13], ss_ext_discount_amt_node_10=[$14], ss_ext_sales_price_node_10=[$15], ss_ext_wholesale_cost_node_10=[$16], ss_ext_list_price_node_10=[$17], ss_ext_tax_node_10=[$18], ss_coupon_amt_node_10=[$19], ss_net_paid_node_10=[$20], ss_net_paid_inc_tax_node_10=[$21], ss_net_profit_node_10=[$22])
                     +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ss_net_paid_inc_tax_node_10])
+- HashAggregate(isMerge=[false], groupBy=[ss_net_profit_node_10], select=[ss_net_profit_node_10, MAX(ss_net_paid_inc_tax_node_10) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_net_profit_node_10]])
      +- HashJoin(joinType=[InnerJoin], where=[=(ss_net_paid_inc_tax_node_100, wr_fee_node_9)], select=[wr_fee_node_9, ss_sold_date_sk_node_10, ss_sold_time_sk_node_10, ss_item_sk_node_10, ss_customer_sk_node_10, ss_cdemo_sk_node_10, ss_hdemo_sk_node_10, ss_addr_sk_node_10, ss_store_sk_node_10, ss_promo_sk_node_10, ss_ticket_number_node_10, ss_quantity_node_10, ss_wholesale_cost_node_10, ss_list_price_node_10, ss_sales_price_node_10, ss_ext_discount_amt_node_10, ss_ext_sales_price_node_10, ss_ext_wholesale_cost_node_10, ss_ext_list_price_node_10, ss_ext_tax_node_10, ss_coupon_amt_node_10, ss_net_paid_node_10, ss_net_paid_inc_tax_node_10, ss_net_profit_node_10, ss_net_paid_inc_tax_node_100], isBroadcast=[true], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[EXPR$0 AS wr_fee_node_9])
         :     +- HashAggregate(isMerge=[false], groupBy=[wr_return_tax], select=[wr_return_tax, AVG(wr_fee) AS EXPR$0])
         :        +- Exchange(distribution=[hash[wr_return_tax]])
         :           +- HashJoin(joinType=[InnerJoin], where=[=(wr_refunded_addr_sk, cd_dep_count)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], build=[right])
         :              :- Exchange(distribution=[hash[cd_dep_count]])
         :              :  +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
         :              +- Exchange(distribution=[hash[wr_refunded_addr_sk]])
         :                 +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
         +- Calc(select=[ss_sold_date_sk AS ss_sold_date_sk_node_10, ss_sold_time_sk AS ss_sold_time_sk_node_10, ss_item_sk AS ss_item_sk_node_10, ss_customer_sk AS ss_customer_sk_node_10, ss_cdemo_sk AS ss_cdemo_sk_node_10, ss_hdemo_sk AS ss_hdemo_sk_node_10, ss_addr_sk AS ss_addr_sk_node_10, ss_store_sk AS ss_store_sk_node_10, ss_promo_sk AS ss_promo_sk_node_10, ss_ticket_number AS ss_ticket_number_node_10, ss_quantity AS ss_quantity_node_10, ss_wholesale_cost AS ss_wholesale_cost_node_10, ss_list_price AS ss_list_price_node_10, ss_sales_price AS ss_sales_price_node_10, ss_ext_discount_amt AS ss_ext_discount_amt_node_10, ss_ext_sales_price AS ss_ext_sales_price_node_10, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_10, ss_ext_list_price AS ss_ext_list_price_node_10, ss_ext_tax AS ss_ext_tax_node_10, ss_coupon_amt AS ss_coupon_amt_node_10, ss_net_paid AS ss_net_paid_node_10, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_10, ss_net_profit AS ss_net_profit_node_10, CAST(ss_net_paid_inc_tax AS DECIMAL(38, 6)) AS ss_net_paid_inc_tax_node_100], where=[>(ss_sold_time_sk, 22)])
            +- Sort(orderBy=[ss_coupon_amt ASC])
               +- Exchange(distribution=[single])
                  +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ss_net_paid_inc_tax_node_10])
+- HashAggregate(isMerge=[false], groupBy=[ss_net_profit_node_10], select=[ss_net_profit_node_10, MAX(ss_net_paid_inc_tax_node_10) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_net_profit_node_10]])
      +- HashJoin(joinType=[InnerJoin], where=[(ss_net_paid_inc_tax_node_100 = wr_fee_node_9)], select=[wr_fee_node_9, ss_sold_date_sk_node_10, ss_sold_time_sk_node_10, ss_item_sk_node_10, ss_customer_sk_node_10, ss_cdemo_sk_node_10, ss_hdemo_sk_node_10, ss_addr_sk_node_10, ss_store_sk_node_10, ss_promo_sk_node_10, ss_ticket_number_node_10, ss_quantity_node_10, ss_wholesale_cost_node_10, ss_list_price_node_10, ss_sales_price_node_10, ss_ext_discount_amt_node_10, ss_ext_sales_price_node_10, ss_ext_wholesale_cost_node_10, ss_ext_list_price_node_10, ss_ext_tax_node_10, ss_coupon_amt_node_10, ss_net_paid_node_10, ss_net_paid_inc_tax_node_10, ss_net_profit_node_10, ss_net_paid_inc_tax_node_100], isBroadcast=[true], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[EXPR$0 AS wr_fee_node_9])
         :     +- HashAggregate(isMerge=[false], groupBy=[wr_return_tax], select=[wr_return_tax, AVG(wr_fee) AS EXPR$0])
         :        +- Exchange(distribution=[hash[wr_return_tax]])
         :           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(wr_refunded_addr_sk = cd_dep_count)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], build=[right])
         :              :- Exchange(distribution=[hash[cd_dep_count]])
         :              :  +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
         :              +- Exchange(distribution=[hash[wr_refunded_addr_sk]])
         :                 +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
         +- Calc(select=[ss_sold_date_sk AS ss_sold_date_sk_node_10, ss_sold_time_sk AS ss_sold_time_sk_node_10, ss_item_sk AS ss_item_sk_node_10, ss_customer_sk AS ss_customer_sk_node_10, ss_cdemo_sk AS ss_cdemo_sk_node_10, ss_hdemo_sk AS ss_hdemo_sk_node_10, ss_addr_sk AS ss_addr_sk_node_10, ss_store_sk AS ss_store_sk_node_10, ss_promo_sk AS ss_promo_sk_node_10, ss_ticket_number AS ss_ticket_number_node_10, ss_quantity AS ss_quantity_node_10, ss_wholesale_cost AS ss_wholesale_cost_node_10, ss_list_price AS ss_list_price_node_10, ss_sales_price AS ss_sales_price_node_10, ss_ext_discount_amt AS ss_ext_discount_amt_node_10, ss_ext_sales_price AS ss_ext_sales_price_node_10, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_10, ss_ext_list_price AS ss_ext_list_price_node_10, ss_ext_tax AS ss_ext_tax_node_10, ss_coupon_amt AS ss_coupon_amt_node_10, ss_net_paid AS ss_net_paid_node_10, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_10, ss_net_profit AS ss_net_profit_node_10, CAST(ss_net_paid_inc_tax AS DECIMAL(38, 6)) AS ss_net_paid_inc_tax_node_100], where=[(ss_sold_time_sk > 22)])
            +- Sort(orderBy=[ss_coupon_amt ASC])
               +- Exchange(distribution=[single])
                  +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o203690950.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#412522597:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[19](input=RelSubset#412522595,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[19]), rel#412522594:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[19](input=RelSubset#412522593,groupBy=ss_net_profit_node_10, ss_net_paid_inc_tax_node_100,select=ss_net_profit_node_10, ss_net_paid_inc_tax_node_100, Partial_MAX(ss_net_paid_inc_tax_node_10) AS max$0)]
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