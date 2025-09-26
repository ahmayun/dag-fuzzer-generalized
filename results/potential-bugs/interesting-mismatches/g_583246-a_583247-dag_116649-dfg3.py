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

autonode_9 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_8 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_7 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_6 = autonode_9.alias('zTVLb')
autonode_5 = autonode_8.order_by(col('ss_sold_date_sk_node_8'))
autonode_4 = autonode_7.order_by(col('cp_catalog_page_number_node_7'))
autonode_3 = autonode_6.filter(col('ss_item_sk_node_9') > 4)
autonode_2 = autonode_4.join(autonode_5, col('ss_promo_sk_node_8') == col('cp_catalog_page_number_node_7'))
autonode_1 = autonode_2.join(autonode_3, col('ss_promo_sk_node_8') == col('ss_store_sk_node_9'))
sink = autonode_1.group_by(col('cp_start_date_sk_node_7')).select(col('cp_start_date_sk_node_7').avg.alias('cp_start_date_sk_node_7'))
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
      "error_message": "An error occurred while calling o318111585.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#641306312:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[6](input=RelSubset#641306310,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[6]), rel#641306309:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[6](input=RelSubset#641306308,groupBy=cp_start_date_sk, cp_catalog_page_number,select=cp_start_date_sk, cp_catalog_page_number)]
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
LogicalProject(cp_start_date_sk_node_7=[$1])
+- LogicalAggregate(group=[{2}], EXPR$0=[AVG($2)])
   +- LogicalJoin(condition=[=($17, $39)], joinType=[inner])
      :- LogicalJoin(condition=[=($17, $6)], joinType=[inner])
      :  :- LogicalSort(sort0=[$6], dir0=[ASC])
      :  :  +- LogicalProject(cp_catalog_page_sk_node_7=[$0], cp_catalog_page_id_node_7=[$1], cp_start_date_sk_node_7=[$2], cp_end_date_sk_node_7=[$3], cp_department_node_7=[$4], cp_catalog_number_node_7=[$5], cp_catalog_page_number_node_7=[$6], cp_description_node_7=[$7], cp_type_node_7=[$8])
      :  :     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])
      :  +- LogicalSort(sort0=[$0], dir0=[ASC])
      :     +- LogicalProject(ss_sold_date_sk_node_8=[$0], ss_sold_time_sk_node_8=[$1], ss_item_sk_node_8=[$2], ss_customer_sk_node_8=[$3], ss_cdemo_sk_node_8=[$4], ss_hdemo_sk_node_8=[$5], ss_addr_sk_node_8=[$6], ss_store_sk_node_8=[$7], ss_promo_sk_node_8=[$8], ss_ticket_number_node_8=[$9], ss_quantity_node_8=[$10], ss_wholesale_cost_node_8=[$11], ss_list_price_node_8=[$12], ss_sales_price_node_8=[$13], ss_ext_discount_amt_node_8=[$14], ss_ext_sales_price_node_8=[$15], ss_ext_wholesale_cost_node_8=[$16], ss_ext_list_price_node_8=[$17], ss_ext_tax_node_8=[$18], ss_coupon_amt_node_8=[$19], ss_net_paid_node_8=[$20], ss_net_paid_inc_tax_node_8=[$21], ss_net_profit_node_8=[$22])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalFilter(condition=[>($2, 4)])
         +- LogicalProject(zTVLb=[AS($0, _UTF-16LE'zTVLb')], ss_sold_time_sk_node_9=[$1], ss_item_sk_node_9=[$2], ss_customer_sk_node_9=[$3], ss_cdemo_sk_node_9=[$4], ss_hdemo_sk_node_9=[$5], ss_addr_sk_node_9=[$6], ss_store_sk_node_9=[$7], ss_promo_sk_node_9=[$8], ss_ticket_number_node_9=[$9], ss_quantity_node_9=[$10], ss_wholesale_cost_node_9=[$11], ss_list_price_node_9=[$12], ss_sales_price_node_9=[$13], ss_ext_discount_amt_node_9=[$14], ss_ext_sales_price_node_9=[$15], ss_ext_wholesale_cost_node_9=[$16], ss_ext_list_price_node_9=[$17], ss_ext_tax_node_9=[$18], ss_coupon_amt_node_9=[$19], ss_net_paid_node_9=[$20], ss_net_paid_inc_tax_node_9=[$21], ss_net_profit_node_9=[$22])
            +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
SortAggregate(isMerge=[false], groupBy=[cp_start_date_sk], select=[cp_start_date_sk])
+- Sort(orderBy=[cp_start_date_sk ASC])
   +- Exchange(distribution=[hash[cp_start_date_sk]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_promo_sk, ss_store_sk)], select=[cp_start_date_sk, ss_promo_sk, ss_store_sk], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- SortAggregate(isMerge=[false], groupBy=[cp_start_date_sk, ss_promo_sk], select=[cp_start_date_sk, ss_promo_sk])
         :     +- Sort(orderBy=[cp_start_date_sk ASC, ss_promo_sk ASC])
         :        +- Exchange(distribution=[hash[cp_start_date_sk, ss_promo_sk]])
         :           +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_promo_sk, cp_catalog_page_number)], select=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
         :              :- Exchange(distribution=[broadcast])
         :              :  +- SortLimit(orderBy=[cp_catalog_page_number ASC], offset=[0], fetch=[1], global=[true])
         :              :     +- Exchange(distribution=[single])
         :              :        +- SortLimit(orderBy=[cp_catalog_page_number ASC], offset=[0], fetch=[1], global=[false])
         :              :           +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
         :              +- SortLimit(orderBy=[ss_sold_date_sk ASC], offset=[0], fetch=[1], global=[true])
         :                 +- Exchange(distribution=[single])
         :                    +- SortLimit(orderBy=[ss_sold_date_sk ASC], offset=[0], fetch=[1], global=[false])
         :                       +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
         +- HashAggregate(isMerge=[true], groupBy=[ss_store_sk], select=[ss_store_sk])
            +- Exchange(distribution=[hash[ss_store_sk]])
               +- LocalHashAggregate(groupBy=[ss_store_sk], select=[ss_store_sk])
                  +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[>(ss_item_sk, 4)])
                     +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[>(ss_item_sk, 4)]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

== Optimized Execution Plan ==
SortAggregate(isMerge=[false], groupBy=[cp_start_date_sk], select=[cp_start_date_sk])
+- Exchange(distribution=[forward])
   +- Sort(orderBy=[cp_start_date_sk ASC])
      +- Exchange(distribution=[hash[cp_start_date_sk]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_promo_sk = ss_store_sk)], select=[cp_start_date_sk, ss_promo_sk, ss_store_sk], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- SortAggregate(isMerge=[false], groupBy=[cp_start_date_sk, ss_promo_sk], select=[cp_start_date_sk, ss_promo_sk])
            :     +- Exchange(distribution=[forward])
            :        +- Sort(orderBy=[cp_start_date_sk ASC, ss_promo_sk ASC])
            :           +- Exchange(distribution=[hash[cp_start_date_sk, ss_promo_sk]])
            :              +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_promo_sk = cp_catalog_page_number)], select=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
            :                 :- Exchange(distribution=[broadcast])
            :                 :  +- SortLimit(orderBy=[cp_catalog_page_number ASC], offset=[0], fetch=[1], global=[true])
            :                 :     +- Exchange(distribution=[single])
            :                 :        +- SortLimit(orderBy=[cp_catalog_page_number ASC], offset=[0], fetch=[1], global=[false])
            :                 :           +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
            :                 +- SortLimit(orderBy=[ss_sold_date_sk ASC], offset=[0], fetch=[1], global=[true])
            :                    +- Exchange(distribution=[single])
            :                       +- SortLimit(orderBy=[ss_sold_date_sk ASC], offset=[0], fetch=[1], global=[false])
            :                          +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- HashAggregate(isMerge=[true], groupBy=[ss_store_sk], select=[ss_store_sk])
               +- Exchange(distribution=[hash[ss_store_sk]])
                  +- LocalHashAggregate(groupBy=[ss_store_sk], select=[ss_store_sk])
                     +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[(ss_item_sk > 4)])
                        +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[>(ss_item_sk, 4)]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0