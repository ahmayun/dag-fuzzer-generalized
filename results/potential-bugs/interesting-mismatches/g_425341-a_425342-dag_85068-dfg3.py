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
    return values.mean()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_5 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_4 = autonode_6.order_by(col('ws_bill_cdemo_sk_node_6'))
autonode_3 = autonode_5.group_by(col('cd_education_status_node_5')).select(col('cd_demo_sk_node_5').sum.alias('cd_demo_sk_node_5'))
autonode_2 = autonode_3.join(autonode_4, col('ws_ship_date_sk_node_6') == col('cd_demo_sk_node_5'))
autonode_1 = autonode_2.group_by(col('ws_ship_mode_sk_node_6')).select(col('ws_ext_ship_cost_node_6').min.alias('ws_ext_ship_cost_node_6'))
sink = autonode_1.order_by(col('ws_ext_ship_cost_node_6'))
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
LogicalSort(sort0=[$0], dir0=[ASC])
+- LogicalProject(ws_ext_ship_cost_node_6=[$1])
   +- LogicalAggregate(group=[{15}], EXPR$0=[MIN($29)])
      +- LogicalJoin(condition=[=($3, $0)], joinType=[inner])
         :- LogicalProject(cd_demo_sk_node_5=[$1])
         :  +- LogicalAggregate(group=[{1}], EXPR$0=[SUM($0)])
         :     +- LogicalProject(cd_demo_sk_node_5=[$0], cd_education_status_node_5=[$3])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
         +- LogicalSort(sort0=[$5], dir0=[ASC])
            +- LogicalProject(ws_sold_date_sk_node_6=[$0], ws_sold_time_sk_node_6=[$1], ws_ship_date_sk_node_6=[$2], ws_item_sk_node_6=[$3], ws_bill_customer_sk_node_6=[$4], ws_bill_cdemo_sk_node_6=[$5], ws_bill_hdemo_sk_node_6=[$6], ws_bill_addr_sk_node_6=[$7], ws_ship_customer_sk_node_6=[$8], ws_ship_cdemo_sk_node_6=[$9], ws_ship_hdemo_sk_node_6=[$10], ws_ship_addr_sk_node_6=[$11], ws_web_page_sk_node_6=[$12], ws_web_site_sk_node_6=[$13], ws_ship_mode_sk_node_6=[$14], ws_warehouse_sk_node_6=[$15], ws_promo_sk_node_6=[$16], ws_order_number_node_6=[$17], ws_quantity_node_6=[$18], ws_wholesale_cost_node_6=[$19], ws_list_price_node_6=[$20], ws_sales_price_node_6=[$21], ws_ext_discount_amt_node_6=[$22], ws_ext_sales_price_node_6=[$23], ws_ext_wholesale_cost_node_6=[$24], ws_ext_list_price_node_6=[$25], ws_ext_tax_node_6=[$26], ws_coupon_amt_node_6=[$27], ws_ext_ship_cost_node_6=[$28], ws_net_paid_node_6=[$29], ws_net_paid_inc_tax_node_6=[$30], ws_net_paid_inc_ship_node_6=[$31], ws_net_paid_inc_ship_tax_node_6=[$32], ws_net_profit_node_6=[$33])
               +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])

== Optimized Physical Plan ==
Sort(orderBy=[ws_ext_ship_cost_node_6 ASC])
+- Exchange(distribution=[single])
   +- Calc(select=[EXPR$0 AS ws_ext_ship_cost_node_6])
      +- HashAggregate(isMerge=[true], groupBy=[ws_ship_mode_sk], select=[ws_ship_mode_sk, Final_MIN(min$0) AS EXPR$0])
         +- Exchange(distribution=[hash[ws_ship_mode_sk]])
            +- LocalHashAggregate(groupBy=[ws_ship_mode_sk], select=[ws_ship_mode_sk, Partial_MIN(ws_ext_ship_cost) AS min$0])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ws_ship_date_sk, cd_demo_sk_node_5)], select=[cd_demo_sk_node_5, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[EXPR$0 AS cd_demo_sk_node_5])
                  :     +- HashAggregate(isMerge=[true], groupBy=[cd_education_status], select=[cd_education_status, Final_SUM(sum$0) AS EXPR$0])
                  :        +- Exchange(distribution=[hash[cd_education_status]])
                  :           +- LocalHashAggregate(groupBy=[cd_education_status], select=[cd_education_status, Partial_SUM(cd_demo_sk) AS sum$0])
                  :              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, project=[cd_demo_sk, cd_education_status], metadata=[]]], fields=[cd_demo_sk, cd_education_status])
                  +- Sort(orderBy=[ws_bill_cdemo_sk ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])

== Optimized Execution Plan ==
Sort(orderBy=[ws_ext_ship_cost_node_6 ASC])
+- Exchange(distribution=[single])
   +- Calc(select=[EXPR$0 AS ws_ext_ship_cost_node_6])
      +- HashAggregate(isMerge=[true], groupBy=[ws_ship_mode_sk], select=[ws_ship_mode_sk, Final_MIN(min$0) AS EXPR$0])
         +- Exchange(distribution=[hash[ws_ship_mode_sk]])
            +- LocalHashAggregate(groupBy=[ws_ship_mode_sk], select=[ws_ship_mode_sk, Partial_MIN(ws_ext_ship_cost) AS min$0])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[(ws_ship_date_sk = cd_demo_sk_node_5)], select=[cd_demo_sk_node_5, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[EXPR$0 AS cd_demo_sk_node_5])
                  :     +- HashAggregate(isMerge=[true], groupBy=[cd_education_status], select=[cd_education_status, Final_SUM(sum$0) AS EXPR$0])
                  :        +- Exchange(distribution=[hash[cd_education_status]])
                  :           +- LocalHashAggregate(groupBy=[cd_education_status], select=[cd_education_status, Partial_SUM(cd_demo_sk) AS sum$0])
                  :              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, project=[cd_demo_sk, cd_education_status], metadata=[]]], fields=[cd_demo_sk, cd_education_status])
                  +- Sort(orderBy=[ws_bill_cdemo_sk ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o231621680.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#468431908:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[5](input=RelSubset#468431906,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[5]), rel#468431905:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[5](input=RelSubset#468431904,groupBy=ws_ship_date_sk, ws_ship_mode_sk,select=ws_ship_date_sk, ws_ship_mode_sk, Partial_MIN(ws_ext_ship_cost) AS min$0)]
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