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
    return values.quantile(0.75)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_9 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_8 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_7 = autonode_9.join(autonode_10, col('ws_web_site_sk_node_9') == col('hd_income_band_sk_node_10'))
autonode_6 = autonode_8.distinct()
autonode_5 = autonode_7.group_by(col('ws_ext_list_price_node_9')).select(col('ws_ship_customer_sk_node_9').avg.alias('ws_ship_customer_sk_node_9'))
autonode_4 = autonode_6.order_by(col('hd_dep_count_node_8'))
autonode_3 = autonode_4.join(autonode_5, col('ws_ship_customer_sk_node_9') == col('hd_demo_sk_node_8'))
autonode_2 = autonode_3.group_by(col('ws_ship_customer_sk_node_9')).select(col('hd_demo_sk_node_8').max.alias('hd_demo_sk_node_8'))
autonode_1 = autonode_2.group_by(col('hd_demo_sk_node_8')).select(col('hd_demo_sk_node_8').min.alias('hd_demo_sk_node_8'))
sink = autonode_1.limit(36)
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
LogicalSort(fetch=[36])
+- LogicalProject(hd_demo_sk_node_8=[$1])
   +- LogicalAggregate(group=[{0}], EXPR$0=[MIN($0)])
      +- LogicalProject(hd_demo_sk_node_8=[$1])
         +- LogicalAggregate(group=[{5}], EXPR$0=[MAX($0)])
            +- LogicalJoin(condition=[=($5, $0)], joinType=[inner])
               :- LogicalSort(sort0=[$3], dir0=[ASC])
               :  +- LogicalAggregate(group=[{0, 1, 2, 3, 4}])
               :     +- LogicalProject(hd_demo_sk_node_8=[$0], hd_income_band_sk_node_8=[$1], hd_buy_potential_node_8=[$2], hd_dep_count_node_8=[$3], hd_vehicle_count_node_8=[$4])
               :        +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
               +- LogicalProject(ws_ship_customer_sk_node_9=[$1])
                  +- LogicalAggregate(group=[{25}], EXPR$0=[AVG($8)])
                     +- LogicalJoin(condition=[=($13, $35)], joinType=[inner])
                        :- LogicalProject(ws_sold_date_sk_node_9=[$0], ws_sold_time_sk_node_9=[$1], ws_ship_date_sk_node_9=[$2], ws_item_sk_node_9=[$3], ws_bill_customer_sk_node_9=[$4], ws_bill_cdemo_sk_node_9=[$5], ws_bill_hdemo_sk_node_9=[$6], ws_bill_addr_sk_node_9=[$7], ws_ship_customer_sk_node_9=[$8], ws_ship_cdemo_sk_node_9=[$9], ws_ship_hdemo_sk_node_9=[$10], ws_ship_addr_sk_node_9=[$11], ws_web_page_sk_node_9=[$12], ws_web_site_sk_node_9=[$13], ws_ship_mode_sk_node_9=[$14], ws_warehouse_sk_node_9=[$15], ws_promo_sk_node_9=[$16], ws_order_number_node_9=[$17], ws_quantity_node_9=[$18], ws_wholesale_cost_node_9=[$19], ws_list_price_node_9=[$20], ws_sales_price_node_9=[$21], ws_ext_discount_amt_node_9=[$22], ws_ext_sales_price_node_9=[$23], ws_ext_wholesale_cost_node_9=[$24], ws_ext_list_price_node_9=[$25], ws_ext_tax_node_9=[$26], ws_coupon_amt_node_9=[$27], ws_ext_ship_cost_node_9=[$28], ws_net_paid_node_9=[$29], ws_net_paid_inc_tax_node_9=[$30], ws_net_paid_inc_ship_node_9=[$31], ws_net_paid_inc_ship_tax_node_9=[$32], ws_net_profit_node_9=[$33])
                        :  +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
                        +- LogicalProject(hd_demo_sk_node_10=[$0], hd_income_band_sk_node_10=[$1], hd_buy_potential_node_10=[$2], hd_dep_count_node_10=[$3], hd_vehicle_count_node_10=[$4])
                           +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[36], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[36], global=[false])
      +- Calc(select=[EXPR$0 AS hd_demo_sk_node_8])
         +- HashAggregate(isMerge=[true], groupBy=[hd_demo_sk_node_8], select=[hd_demo_sk_node_8, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[hash[hd_demo_sk_node_8]])
               +- LocalHashAggregate(groupBy=[hd_demo_sk_node_8], select=[hd_demo_sk_node_8, Partial_MIN(hd_demo_sk_node_8) AS min$0])
                  +- Calc(select=[EXPR$0 AS hd_demo_sk_node_8])
                     +- HashAggregate(isMerge=[true], groupBy=[ws_ship_customer_sk_node_9], select=[ws_ship_customer_sk_node_9, Final_MAX(max$0) AS EXPR$0])
                        +- Exchange(distribution=[hash[ws_ship_customer_sk_node_9]])
                           +- LocalHashAggregate(groupBy=[ws_ship_customer_sk_node_9], select=[ws_ship_customer_sk_node_9, Partial_MAX(hd_demo_sk) AS max$0])
                              +- HashJoin(joinType=[InnerJoin], where=[=(ws_ship_customer_sk_node_9, hd_demo_sk)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ws_ship_customer_sk_node_9], isBroadcast=[true], build=[right])
                                 :- Sort(orderBy=[hd_dep_count ASC])
                                 :  +- Exchange(distribution=[single])
                                 :     +- HashAggregate(isMerge=[true], groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                 :        +- Exchange(distribution=[hash[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count]])
                                 :           +- LocalHashAggregate(groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                 :              +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                 +- Exchange(distribution=[broadcast])
                                    +- Calc(select=[EXPR$0 AS ws_ship_customer_sk_node_9])
                                       +- HashAggregate(isMerge=[false], groupBy=[ws_ext_list_price], select=[ws_ext_list_price, AVG(ws_ship_customer_sk) AS EXPR$0])
                                          +- Exchange(distribution=[hash[ws_ext_list_price]])
                                             +- HashJoin(joinType=[InnerJoin], where=[=(ws_web_site_sk, hd_income_band_sk)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], isBroadcast=[true], build=[right])
                                                :- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                                                +- Exchange(distribution=[broadcast])
                                                   +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[36], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[36], global=[false])
      +- Calc(select=[EXPR$0 AS hd_demo_sk_node_8])
         +- HashAggregate(isMerge=[true], groupBy=[hd_demo_sk_node_8], select=[hd_demo_sk_node_8, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[hash[hd_demo_sk_node_8]])
               +- LocalHashAggregate(groupBy=[hd_demo_sk_node_8], select=[hd_demo_sk_node_8, Partial_MIN(hd_demo_sk_node_8) AS min$0])
                  +- Calc(select=[EXPR$0 AS hd_demo_sk_node_8])
                     +- HashAggregate(isMerge=[true], groupBy=[ws_ship_customer_sk_node_9], select=[ws_ship_customer_sk_node_9, Final_MAX(max$0) AS EXPR$0])
                        +- Exchange(distribution=[hash[ws_ship_customer_sk_node_9]])
                           +- LocalHashAggregate(groupBy=[ws_ship_customer_sk_node_9], select=[ws_ship_customer_sk_node_9, Partial_MAX(hd_demo_sk) AS max$0])
                              +- HashJoin(joinType=[InnerJoin], where=[(ws_ship_customer_sk_node_9 = hd_demo_sk)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ws_ship_customer_sk_node_9], isBroadcast=[true], build=[right])
                                 :- Sort(orderBy=[hd_dep_count ASC])
                                 :  +- Exchange(distribution=[single])
                                 :     +- HashAggregate(isMerge=[true], groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                 :        +- Exchange(distribution=[hash[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count]])
                                 :           +- LocalHashAggregate(groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                 :              +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])(reuse_id=[1])
                                 +- Exchange(distribution=[broadcast])
                                    +- Calc(select=[EXPR$0 AS ws_ship_customer_sk_node_9])
                                       +- HashAggregate(isMerge=[false], groupBy=[ws_ext_list_price], select=[ws_ext_list_price, AVG(ws_ship_customer_sk) AS EXPR$0])
                                          +- Exchange(distribution=[hash[ws_ext_list_price]])
                                             +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(ws_web_site_sk = hd_income_band_sk)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])\
+- [#2] Exchange(distribution=[broadcast])\
])
                                                :- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                                                +- Exchange(distribution=[broadcast])
                                                   +- Reused(reference_id=[1])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o326362747.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#658335778:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[3](input=RelSubset#658335776,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[3]), rel#658335775:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#658335774,groupBy=hd_demo_sk,select=hd_demo_sk, Partial_MAX(hd_demo_sk) AS max$0)]
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