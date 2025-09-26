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
    return values.iloc[0] if len(values) > 0 else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_9 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_8 = autonode_10.filter(col('ss_sales_price_node_10') > -30.475831031799316)
autonode_7 = autonode_9.order_by(col('ws_coupon_amt_node_9'))
autonode_6 = autonode_8.select(col('ss_coupon_amt_node_10'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_4 = autonode_5.join(autonode_6, col('ws_net_paid_inc_ship_tax_node_9') == col('ss_coupon_amt_node_10'))
autonode_3 = autonode_4.group_by(col('ws_ext_ship_cost_node_9')).select(col('ws_ship_date_sk_node_9').min.alias('ws_ship_date_sk_node_9'))
autonode_2 = autonode_3.add_columns(lit("hello"))
autonode_1 = autonode_2.order_by(col('ws_ship_date_sk_node_9'))
sink = autonode_1.distinct()
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
LogicalAggregate(group=[{0, 1}])
+- LogicalSort(sort0=[$0], dir0=[ASC])
   +- LogicalProject(ws_ship_date_sk_node_9=[$1], _c1=[_UTF-16LE'hello'])
      +- LogicalAggregate(group=[{28}], EXPR$0=[MIN($2)])
         +- LogicalJoin(condition=[=($32, $35)], joinType=[inner])
            :- LogicalProject(ws_sold_date_sk_node_9=[$0], ws_sold_time_sk_node_9=[$1], ws_ship_date_sk_node_9=[$2], ws_item_sk_node_9=[$3], ws_bill_customer_sk_node_9=[$4], ws_bill_cdemo_sk_node_9=[$5], ws_bill_hdemo_sk_node_9=[$6], ws_bill_addr_sk_node_9=[$7], ws_ship_customer_sk_node_9=[$8], ws_ship_cdemo_sk_node_9=[$9], ws_ship_hdemo_sk_node_9=[$10], ws_ship_addr_sk_node_9=[$11], ws_web_page_sk_node_9=[$12], ws_web_site_sk_node_9=[$13], ws_ship_mode_sk_node_9=[$14], ws_warehouse_sk_node_9=[$15], ws_promo_sk_node_9=[$16], ws_order_number_node_9=[$17], ws_quantity_node_9=[$18], ws_wholesale_cost_node_9=[$19], ws_list_price_node_9=[$20], ws_sales_price_node_9=[$21], ws_ext_discount_amt_node_9=[$22], ws_ext_sales_price_node_9=[$23], ws_ext_wholesale_cost_node_9=[$24], ws_ext_list_price_node_9=[$25], ws_ext_tax_node_9=[$26], ws_coupon_amt_node_9=[$27], ws_ext_ship_cost_node_9=[$28], ws_net_paid_node_9=[$29], ws_net_paid_inc_tax_node_9=[$30], ws_net_paid_inc_ship_node_9=[$31], ws_net_paid_inc_ship_tax_node_9=[$32], ws_net_profit_node_9=[$33], _c34=[_UTF-16LE'hello'])
            :  +- LogicalSort(sort0=[$27], dir0=[ASC])
            :     +- LogicalProject(ws_sold_date_sk_node_9=[$0], ws_sold_time_sk_node_9=[$1], ws_ship_date_sk_node_9=[$2], ws_item_sk_node_9=[$3], ws_bill_customer_sk_node_9=[$4], ws_bill_cdemo_sk_node_9=[$5], ws_bill_hdemo_sk_node_9=[$6], ws_bill_addr_sk_node_9=[$7], ws_ship_customer_sk_node_9=[$8], ws_ship_cdemo_sk_node_9=[$9], ws_ship_hdemo_sk_node_9=[$10], ws_ship_addr_sk_node_9=[$11], ws_web_page_sk_node_9=[$12], ws_web_site_sk_node_9=[$13], ws_ship_mode_sk_node_9=[$14], ws_warehouse_sk_node_9=[$15], ws_promo_sk_node_9=[$16], ws_order_number_node_9=[$17], ws_quantity_node_9=[$18], ws_wholesale_cost_node_9=[$19], ws_list_price_node_9=[$20], ws_sales_price_node_9=[$21], ws_ext_discount_amt_node_9=[$22], ws_ext_sales_price_node_9=[$23], ws_ext_wholesale_cost_node_9=[$24], ws_ext_list_price_node_9=[$25], ws_ext_tax_node_9=[$26], ws_coupon_amt_node_9=[$27], ws_ext_ship_cost_node_9=[$28], ws_net_paid_node_9=[$29], ws_net_paid_inc_tax_node_9=[$30], ws_net_paid_inc_ship_node_9=[$31], ws_net_paid_inc_ship_tax_node_9=[$32], ws_net_profit_node_9=[$33])
            :        +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
            +- LogicalProject(ss_coupon_amt_node_10=[$19])
               +- LogicalFilter(condition=[>($13, -3.0475831031799316E1:DOUBLE)])
                  +- LogicalProject(ss_sold_date_sk_node_10=[$0], ss_sold_time_sk_node_10=[$1], ss_item_sk_node_10=[$2], ss_customer_sk_node_10=[$3], ss_cdemo_sk_node_10=[$4], ss_hdemo_sk_node_10=[$5], ss_addr_sk_node_10=[$6], ss_store_sk_node_10=[$7], ss_promo_sk_node_10=[$8], ss_ticket_number_node_10=[$9], ss_quantity_node_10=[$10], ss_wholesale_cost_node_10=[$11], ss_list_price_node_10=[$12], ss_sales_price_node_10=[$13], ss_ext_discount_amt_node_10=[$14], ss_ext_sales_price_node_10=[$15], ss_ext_wholesale_cost_node_10=[$16], ss_ext_list_price_node_10=[$17], ss_ext_tax_node_10=[$18], ss_coupon_amt_node_10=[$19], ss_net_paid_node_10=[$20], ss_net_paid_inc_tax_node_10=[$21], ss_net_profit_node_10=[$22])
                     +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
HashAggregate(isMerge=[true], groupBy=[ws_ship_date_sk_node_9], auxGrouping=[_c1], select=[ws_ship_date_sk_node_9, _c1])
+- Sort(orderBy=[ws_ship_date_sk_node_9 ASC])
   +- Exchange(distribution=[hash[ws_ship_date_sk_node_9]])
      +- LocalHashAggregate(groupBy=[ws_ship_date_sk_node_9], auxGrouping=[_c1], select=[ws_ship_date_sk_node_9, _c1])
         +- Sort(orderBy=[ws_ship_date_sk_node_9 ASC])
            +- Exchange(distribution=[single])
               +- Calc(select=[EXPR$0 AS ws_ship_date_sk_node_9, 'hello' AS _c1])
                  +- HashAggregate(isMerge=[false], groupBy=[ws_ext_ship_cost_node_9], select=[ws_ext_ship_cost_node_9, MIN(ws_ship_date_sk_node_9) AS EXPR$0])
                     +- Exchange(distribution=[hash[ws_ext_ship_cost_node_9]])
                        +- HashJoin(joinType=[InnerJoin], where=[=(ws_net_paid_inc_ship_tax_node_9, ss_coupon_amt_node_10)], select=[ws_sold_date_sk_node_9, ws_sold_time_sk_node_9, ws_ship_date_sk_node_9, ws_item_sk_node_9, ws_bill_customer_sk_node_9, ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk_node_9, ws_bill_addr_sk_node_9, ws_ship_customer_sk_node_9, ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk_node_9, ws_ship_addr_sk_node_9, ws_web_page_sk_node_9, ws_web_site_sk_node_9, ws_ship_mode_sk_node_9, ws_warehouse_sk_node_9, ws_promo_sk_node_9, ws_order_number_node_9, ws_quantity_node_9, ws_wholesale_cost_node_9, ws_list_price_node_9, ws_sales_price_node_9, ws_ext_discount_amt_node_9, ws_ext_sales_price_node_9, ws_ext_wholesale_cost_node_9, ws_ext_list_price_node_9, ws_ext_tax_node_9, ws_coupon_amt_node_9, ws_ext_ship_cost_node_9, ws_net_paid_node_9, ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax_node_9, ws_net_profit_node_9, _c34, ss_coupon_amt_node_10], build=[right])
                           :- Exchange(distribution=[hash[ws_net_paid_inc_ship_tax_node_9]])
                           :  +- Calc(select=[ws_sold_date_sk AS ws_sold_date_sk_node_9, ws_sold_time_sk AS ws_sold_time_sk_node_9, ws_ship_date_sk AS ws_ship_date_sk_node_9, ws_item_sk AS ws_item_sk_node_9, ws_bill_customer_sk AS ws_bill_customer_sk_node_9, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_9, ws_bill_addr_sk AS ws_bill_addr_sk_node_9, ws_ship_customer_sk AS ws_ship_customer_sk_node_9, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_9, ws_ship_addr_sk AS ws_ship_addr_sk_node_9, ws_web_page_sk AS ws_web_page_sk_node_9, ws_web_site_sk AS ws_web_site_sk_node_9, ws_ship_mode_sk AS ws_ship_mode_sk_node_9, ws_warehouse_sk AS ws_warehouse_sk_node_9, ws_promo_sk AS ws_promo_sk_node_9, ws_order_number AS ws_order_number_node_9, ws_quantity AS ws_quantity_node_9, ws_wholesale_cost AS ws_wholesale_cost_node_9, ws_list_price AS ws_list_price_node_9, ws_sales_price AS ws_sales_price_node_9, ws_ext_discount_amt AS ws_ext_discount_amt_node_9, ws_ext_sales_price AS ws_ext_sales_price_node_9, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_9, ws_ext_list_price AS ws_ext_list_price_node_9, ws_ext_tax AS ws_ext_tax_node_9, ws_coupon_amt AS ws_coupon_amt_node_9, ws_ext_ship_cost AS ws_ext_ship_cost_node_9, ws_net_paid AS ws_net_paid_node_9, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_9, ws_net_profit AS ws_net_profit_node_9, 'hello' AS _c34])
                           :     +- Sort(orderBy=[ws_coupon_amt ASC])
                           :        +- Exchange(distribution=[single])
                           :           +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                           +- Exchange(distribution=[hash[ss_coupon_amt_node_10]])
                              +- Calc(select=[ss_coupon_amt AS ss_coupon_amt_node_10], where=[>(ss_sales_price, -3.0475831031799316E1)])
                                 +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[>(ss_sales_price, -3.0475831031799316E1:DOUBLE)], project=[ss_sales_price, ss_coupon_amt], metadata=[]]], fields=[ss_sales_price, ss_coupon_amt])

== Optimized Execution Plan ==
HashAggregate(isMerge=[true], groupBy=[ws_ship_date_sk_node_9], auxGrouping=[_c1], select=[ws_ship_date_sk_node_9, _c1])
+- Exchange(distribution=[keep_input_as_is[hash[ws_ship_date_sk_node_9]]])
   +- Sort(orderBy=[ws_ship_date_sk_node_9 ASC])
      +- Exchange(distribution=[hash[ws_ship_date_sk_node_9]])
         +- LocalHashAggregate(groupBy=[ws_ship_date_sk_node_9], auxGrouping=[_c1], select=[ws_ship_date_sk_node_9, _c1])
            +- Sort(orderBy=[ws_ship_date_sk_node_9 ASC])
               +- Exchange(distribution=[single])
                  +- Calc(select=[EXPR$0 AS ws_ship_date_sk_node_9, 'hello' AS _c1])
                     +- HashAggregate(isMerge=[false], groupBy=[ws_ext_ship_cost_node_9], select=[ws_ext_ship_cost_node_9, MIN(ws_ship_date_sk_node_9) AS EXPR$0])
                        +- Exchange(distribution=[hash[ws_ext_ship_cost_node_9]])
                           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ws_net_paid_inc_ship_tax_node_9 = ss_coupon_amt_node_10)], select=[ws_sold_date_sk_node_9, ws_sold_time_sk_node_9, ws_ship_date_sk_node_9, ws_item_sk_node_9, ws_bill_customer_sk_node_9, ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk_node_9, ws_bill_addr_sk_node_9, ws_ship_customer_sk_node_9, ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk_node_9, ws_ship_addr_sk_node_9, ws_web_page_sk_node_9, ws_web_site_sk_node_9, ws_ship_mode_sk_node_9, ws_warehouse_sk_node_9, ws_promo_sk_node_9, ws_order_number_node_9, ws_quantity_node_9, ws_wholesale_cost_node_9, ws_list_price_node_9, ws_sales_price_node_9, ws_ext_discount_amt_node_9, ws_ext_sales_price_node_9, ws_ext_wholesale_cost_node_9, ws_ext_list_price_node_9, ws_ext_tax_node_9, ws_coupon_amt_node_9, ws_ext_ship_cost_node_9, ws_net_paid_node_9, ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax_node_9, ws_net_profit_node_9, _c34, ss_coupon_amt_node_10], build=[right])
                              :- Exchange(distribution=[hash[ws_net_paid_inc_ship_tax_node_9]])
                              :  +- Calc(select=[ws_sold_date_sk AS ws_sold_date_sk_node_9, ws_sold_time_sk AS ws_sold_time_sk_node_9, ws_ship_date_sk AS ws_ship_date_sk_node_9, ws_item_sk AS ws_item_sk_node_9, ws_bill_customer_sk AS ws_bill_customer_sk_node_9, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_9, ws_bill_addr_sk AS ws_bill_addr_sk_node_9, ws_ship_customer_sk AS ws_ship_customer_sk_node_9, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_9, ws_ship_addr_sk AS ws_ship_addr_sk_node_9, ws_web_page_sk AS ws_web_page_sk_node_9, ws_web_site_sk AS ws_web_site_sk_node_9, ws_ship_mode_sk AS ws_ship_mode_sk_node_9, ws_warehouse_sk AS ws_warehouse_sk_node_9, ws_promo_sk AS ws_promo_sk_node_9, ws_order_number AS ws_order_number_node_9, ws_quantity AS ws_quantity_node_9, ws_wholesale_cost AS ws_wholesale_cost_node_9, ws_list_price AS ws_list_price_node_9, ws_sales_price AS ws_sales_price_node_9, ws_ext_discount_amt AS ws_ext_discount_amt_node_9, ws_ext_sales_price AS ws_ext_sales_price_node_9, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_9, ws_ext_list_price AS ws_ext_list_price_node_9, ws_ext_tax AS ws_ext_tax_node_9, ws_coupon_amt AS ws_coupon_amt_node_9, ws_ext_ship_cost AS ws_ext_ship_cost_node_9, ws_net_paid AS ws_net_paid_node_9, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_9, ws_net_profit AS ws_net_profit_node_9, 'hello' AS _c34])
                              :     +- Sort(orderBy=[ws_coupon_amt ASC])
                              :        +- Exchange(distribution=[single])
                              :           +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                              +- Exchange(distribution=[hash[ss_coupon_amt_node_10]])
                                 +- Calc(select=[ss_coupon_amt AS ss_coupon_amt_node_10], where=[(ss_sales_price > -3.0475831031799316E1)])
                                    +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[>(ss_sales_price, -3.0475831031799316E1:DOUBLE)], project=[ss_sales_price, ss_coupon_amt], metadata=[]]], fields=[ss_sales_price, ss_coupon_amt])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o121590718.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#245225672:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[27](input=RelSubset#245225670,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[27]), rel#245225669:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[27](input=RelSubset#245225668,groupBy=ws_ext_ship_cost, ws_net_paid_inc_ship_tax,select=ws_ext_ship_cost, ws_net_paid_inc_ship_tax, Partial_MIN(ws_ship_date_sk) AS min$0)]
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