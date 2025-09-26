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

autonode_10 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_12 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_11 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_7 = autonode_10.order_by(col('cs_ext_tax_node_10'))
autonode_9 = autonode_12.filter(preloaded_udf_boolean(col('ws_ship_hdemo_sk_node_12')))
autonode_8 = autonode_11.group_by(col('sr_return_tax_node_11')).select(col('sr_addr_sk_node_11').count.alias('sr_addr_sk_node_11'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_6 = autonode_8.join(autonode_9, col('sr_addr_sk_node_11') == col('ws_ship_hdemo_sk_node_12'))
autonode_4 = autonode_5.join(autonode_6, col('ws_ship_addr_sk_node_12') == col('cs_order_number_node_10'))
autonode_3 = autonode_4.filter(col('cs_ext_wholesale_cost_node_10') < -17.767274379730225)
autonode_2 = autonode_3.group_by(col('ws_sold_date_sk_node_12')).select(col('ws_net_paid_inc_ship_tax_node_12').min.alias('ws_net_paid_inc_ship_tax_node_12'))
autonode_1 = autonode_2.filter(col('ws_net_paid_inc_ship_tax_node_12') <= 18.97379159927368)
sink = autonode_1.limit(40)
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
      "error_message": "An error occurred while calling o39354691.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#78237545:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[26](input=RelSubset#78237543,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[26]), rel#78237542:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[26](input=RelSubset#78237541,groupBy=cs_order_number,select=cs_order_number)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (26) must be less than size (1)
\tat org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkElementIndex(Preconditions.java:1371)
\tat org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkElementIndex(Preconditions.java:1353)
\tat org.apache.flink.calcite.shaded.com.google.common.collect.SingletonImmutableList.get(SingletonImmutableList.java:44)
\tat org.apache.calcite.util.Util$TransformingList.get(Util.java:2794)
\tat scala.collection.convert.Wrappers$JListWrapper.apply(Wrappers.scala:100)
\tat org.apache.flink.table.planner.plan.utils.RelExplainUtil$.$anonfun$collationToString$1(RelExplainUtil.scala:83)
\tat scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
\tat scala.collection.Iterator.foreach(Iterator.scala:943)
\tat scala.collection.Iterator.foreach$(Iterator.scala:943)
\tat scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
\tat scala.collection.IterableLike.foreach(IterableLike.scala:74)
\tat scala.collection.IterableLike.foreach$(IterableLike.scala:73)
\tat scala.collection.AbstractIterable.foreach(Iterable.scala:56)
\tat scala.collection.TraversableLike.map(TraversableLike.scala:286)
\tat scala.collection.TraversableLike.map$(TraversableLike.scala:279)
\tat scala.collection.AbstractTraversable.map(Traversable.scala:108)
\tat org.apache.flink.table.planner.plan.utils.RelExplainUtil$.collationToString(RelExplainUtil.scala:83)
\tat org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSort.explainTerms(BatchPhysicalSort.scala:61)
\tat org.apache.calcite.rel.AbstractRelNode.getDigestItems(AbstractRelNode.java:414)
\tat org.apache.calcite.rel.AbstractRelNode.deepHashCode(AbstractRelNode.java:396)
\tat org.apache.calcite.rel.AbstractRelNode$InnerRelDigest.hashCode(AbstractRelNode.java:448)
\tat java.base/java.util.HashMap.hash(HashMap.java:340)
\tat java.base/java.util.HashMap.get(HashMap.java:553)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.registerImpl(VolcanoPlanner.java:1289)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.register(VolcanoPlanner.java:598)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:613)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.transformTo(VolcanoRuleCall.java:144)
\t... 45 more
",
      "stdout": "",
      "stderr": ""
    },
    "unopt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalSort(fetch=[40])
+- LogicalFilter(condition=[<=($0, 1.897379159927368E1:DOUBLE)])
   +- LogicalProject(ws_net_paid_inc_ship_tax_node_12=[$1])
      +- LogicalAggregate(group=[{36}], EXPR$0=[MIN($68)])
         +- LogicalFilter(condition=[<($24, -1.7767274379730225E1:DOUBLE)])
            +- LogicalJoin(condition=[=($47, $17)], joinType=[inner])
               :- LogicalProject(cs_sold_date_sk_node_10=[$0], cs_sold_time_sk_node_10=[$1], cs_ship_date_sk_node_10=[$2], cs_bill_customer_sk_node_10=[$3], cs_bill_cdemo_sk_node_10=[$4], cs_bill_hdemo_sk_node_10=[$5], cs_bill_addr_sk_node_10=[$6], cs_ship_customer_sk_node_10=[$7], cs_ship_cdemo_sk_node_10=[$8], cs_ship_hdemo_sk_node_10=[$9], cs_ship_addr_sk_node_10=[$10], cs_call_center_sk_node_10=[$11], cs_catalog_page_sk_node_10=[$12], cs_ship_mode_sk_node_10=[$13], cs_warehouse_sk_node_10=[$14], cs_item_sk_node_10=[$15], cs_promo_sk_node_10=[$16], cs_order_number_node_10=[$17], cs_quantity_node_10=[$18], cs_wholesale_cost_node_10=[$19], cs_list_price_node_10=[$20], cs_sales_price_node_10=[$21], cs_ext_discount_amt_node_10=[$22], cs_ext_sales_price_node_10=[$23], cs_ext_wholesale_cost_node_10=[$24], cs_ext_list_price_node_10=[$25], cs_ext_tax_node_10=[$26], cs_coupon_amt_node_10=[$27], cs_ext_ship_cost_node_10=[$28], cs_net_paid_node_10=[$29], cs_net_paid_inc_tax_node_10=[$30], cs_net_paid_inc_ship_node_10=[$31], cs_net_paid_inc_ship_tax_node_10=[$32], cs_net_profit_node_10=[$33], _c34=[_UTF-16LE'hello'])
               :  +- LogicalSort(sort0=[$26], dir0=[ASC])
               :     +- LogicalProject(cs_sold_date_sk_node_10=[$0], cs_sold_time_sk_node_10=[$1], cs_ship_date_sk_node_10=[$2], cs_bill_customer_sk_node_10=[$3], cs_bill_cdemo_sk_node_10=[$4], cs_bill_hdemo_sk_node_10=[$5], cs_bill_addr_sk_node_10=[$6], cs_ship_customer_sk_node_10=[$7], cs_ship_cdemo_sk_node_10=[$8], cs_ship_hdemo_sk_node_10=[$9], cs_ship_addr_sk_node_10=[$10], cs_call_center_sk_node_10=[$11], cs_catalog_page_sk_node_10=[$12], cs_ship_mode_sk_node_10=[$13], cs_warehouse_sk_node_10=[$14], cs_item_sk_node_10=[$15], cs_promo_sk_node_10=[$16], cs_order_number_node_10=[$17], cs_quantity_node_10=[$18], cs_wholesale_cost_node_10=[$19], cs_list_price_node_10=[$20], cs_sales_price_node_10=[$21], cs_ext_discount_amt_node_10=[$22], cs_ext_sales_price_node_10=[$23], cs_ext_wholesale_cost_node_10=[$24], cs_ext_list_price_node_10=[$25], cs_ext_tax_node_10=[$26], cs_coupon_amt_node_10=[$27], cs_ext_ship_cost_node_10=[$28], cs_net_paid_node_10=[$29], cs_net_paid_inc_tax_node_10=[$30], cs_net_paid_inc_ship_node_10=[$31], cs_net_paid_inc_ship_tax_node_10=[$32], cs_net_profit_node_10=[$33])
               :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
               +- LogicalJoin(condition=[=($0, $11)], joinType=[inner])
                  :- LogicalProject(sr_addr_sk_node_11=[$1])
                  :  +- LogicalAggregate(group=[{1}], EXPR$0=[COUNT($0)])
                  :     +- LogicalProject(sr_addr_sk_node_11=[$6], sr_return_tax_node_11=[$12])
                  :        +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
                  +- LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($10)])
                     +- LogicalProject(ws_sold_date_sk_node_12=[$0], ws_sold_time_sk_node_12=[$1], ws_ship_date_sk_node_12=[$2], ws_item_sk_node_12=[$3], ws_bill_customer_sk_node_12=[$4], ws_bill_cdemo_sk_node_12=[$5], ws_bill_hdemo_sk_node_12=[$6], ws_bill_addr_sk_node_12=[$7], ws_ship_customer_sk_node_12=[$8], ws_ship_cdemo_sk_node_12=[$9], ws_ship_hdemo_sk_node_12=[$10], ws_ship_addr_sk_node_12=[$11], ws_web_page_sk_node_12=[$12], ws_web_site_sk_node_12=[$13], ws_ship_mode_sk_node_12=[$14], ws_warehouse_sk_node_12=[$15], ws_promo_sk_node_12=[$16], ws_order_number_node_12=[$17], ws_quantity_node_12=[$18], ws_wholesale_cost_node_12=[$19], ws_list_price_node_12=[$20], ws_sales_price_node_12=[$21], ws_ext_discount_amt_node_12=[$22], ws_ext_sales_price_node_12=[$23], ws_ext_wholesale_cost_node_12=[$24], ws_ext_list_price_node_12=[$25], ws_ext_tax_node_12=[$26], ws_coupon_amt_node_12=[$27], ws_ext_ship_cost_node_12=[$28], ws_net_paid_node_12=[$29], ws_net_paid_inc_tax_node_12=[$30], ws_net_paid_inc_ship_node_12=[$31], ws_net_paid_inc_ship_tax_node_12=[$32], ws_net_profit_node_12=[$33])
                        +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[40], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[40], global=[false])
      +- Calc(select=[EXPR$0], where=[<=(EXPR$0, 1.897379159927368E1)])
         +- HashAggregate(isMerge=[false], groupBy=[ws_sold_date_sk_node_12], select=[ws_sold_date_sk_node_12, MIN(ws_net_paid_inc_ship_tax_node_12) AS EXPR$0])
            +- Exchange(distribution=[hash[ws_sold_date_sk_node_12]])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ws_ship_addr_sk_node_12, cs_order_number_node_10)], select=[cs_sold_date_sk_node_10, cs_sold_time_sk_node_10, cs_ship_date_sk_node_10, cs_bill_customer_sk_node_10, cs_bill_cdemo_sk_node_10, cs_bill_hdemo_sk_node_10, cs_bill_addr_sk_node_10, cs_ship_customer_sk_node_10, cs_ship_cdemo_sk_node_10, cs_ship_hdemo_sk_node_10, cs_ship_addr_sk_node_10, cs_call_center_sk_node_10, cs_catalog_page_sk_node_10, cs_ship_mode_sk_node_10, cs_warehouse_sk_node_10, cs_item_sk_node_10, cs_promo_sk_node_10, cs_order_number_node_10, cs_quantity_node_10, cs_wholesale_cost_node_10, cs_list_price_node_10, cs_sales_price_node_10, cs_ext_discount_amt_node_10, cs_ext_sales_price_node_10, cs_ext_wholesale_cost_node_10, cs_ext_list_price_node_10, cs_ext_tax_node_10, cs_coupon_amt_node_10, cs_ext_ship_cost_node_10, cs_net_paid_node_10, cs_net_paid_inc_tax_node_10, cs_net_paid_inc_ship_node_10, cs_net_paid_inc_ship_tax_node_10, cs_net_profit_node_10, _c34, sr_addr_sk_node_11, ws_sold_date_sk_node_12, ws_sold_time_sk_node_12, ws_ship_date_sk_node_12, ws_item_sk_node_12, ws_bill_customer_sk_node_12, ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk_node_12, ws_bill_addr_sk_node_12, ws_ship_customer_sk_node_12, ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk_node_12, ws_ship_addr_sk_node_12, ws_web_page_sk_node_12, ws_web_site_sk_node_12, ws_ship_mode_sk_node_12, ws_warehouse_sk_node_12, ws_promo_sk_node_12, ws_order_number_node_12, ws_quantity_node_12, ws_wholesale_cost_node_12, ws_list_price_node_12, ws_sales_price_node_12, ws_ext_discount_amt_node_12, ws_ext_sales_price_node_12, ws_ext_wholesale_cost_node_12, ws_ext_list_price_node_12, ws_ext_tax_node_12, ws_coupon_amt_node_12, ws_ext_ship_cost_node_12, ws_net_paid_node_12, ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax_node_12, ws_net_profit_node_12], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[cs_sold_date_sk AS cs_sold_date_sk_node_10, cs_sold_time_sk AS cs_sold_time_sk_node_10, cs_ship_date_sk AS cs_ship_date_sk_node_10, cs_bill_customer_sk AS cs_bill_customer_sk_node_10, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_10, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_10, cs_bill_addr_sk AS cs_bill_addr_sk_node_10, cs_ship_customer_sk AS cs_ship_customer_sk_node_10, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_10, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_10, cs_ship_addr_sk AS cs_ship_addr_sk_node_10, cs_call_center_sk AS cs_call_center_sk_node_10, cs_catalog_page_sk AS cs_catalog_page_sk_node_10, cs_ship_mode_sk AS cs_ship_mode_sk_node_10, cs_warehouse_sk AS cs_warehouse_sk_node_10, cs_item_sk AS cs_item_sk_node_10, cs_promo_sk AS cs_promo_sk_node_10, cs_order_number AS cs_order_number_node_10, cs_quantity AS cs_quantity_node_10, cs_wholesale_cost AS cs_wholesale_cost_node_10, cs_list_price AS cs_list_price_node_10, cs_sales_price AS cs_sales_price_node_10, cs_ext_discount_amt AS cs_ext_discount_amt_node_10, cs_ext_sales_price AS cs_ext_sales_price_node_10, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_10, cs_ext_list_price AS cs_ext_list_price_node_10, cs_ext_tax AS cs_ext_tax_node_10, cs_coupon_amt AS cs_coupon_amt_node_10, cs_ext_ship_cost AS cs_ext_ship_cost_node_10, cs_net_paid AS cs_net_paid_node_10, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_10, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_10, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_10, cs_net_profit AS cs_net_profit_node_10, 'hello' AS _c34], where=[<(cs_ext_wholesale_cost, -1.7767274379730225E1)])
                  :     +- SortLimit(orderBy=[cs_ext_tax ASC], offset=[0], fetch=[1], global=[true])
                  :        +- Exchange(distribution=[single])
                  :           +- SortLimit(orderBy=[cs_ext_tax ASC], offset=[0], fetch=[1], global=[false])
                  :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
                  +- Calc(select=[sr_addr_sk_node_11, ws_sold_date_sk_node_12, ws_sold_time_sk_node_12, ws_ship_date_sk_node_12, ws_item_sk_node_12, ws_bill_customer_sk_node_12, ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk_node_12, ws_bill_addr_sk_node_12, ws_ship_customer_sk_node_12, ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk_node_12, ws_ship_addr_sk_node_12, ws_web_page_sk_node_12, ws_web_site_sk_node_12, ws_ship_mode_sk_node_12, ws_warehouse_sk_node_12, ws_promo_sk_node_12, ws_order_number_node_12, ws_quantity_node_12, ws_wholesale_cost_node_12, ws_list_price_node_12, ws_sales_price_node_12, ws_ext_discount_amt_node_12, ws_ext_sales_price_node_12, ws_ext_wholesale_cost_node_12, ws_ext_list_price_node_12, ws_ext_tax_node_12, ws_coupon_amt_node_12, ws_ext_ship_cost_node_12, ws_net_paid_node_12, ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax_node_12, ws_net_profit_node_12])
                     +- HashJoin(joinType=[InnerJoin], where=[=(sr_addr_sk_node_11, ws_ship_hdemo_sk_node_120)], select=[sr_addr_sk_node_11, ws_sold_date_sk_node_12, ws_sold_time_sk_node_12, ws_ship_date_sk_node_12, ws_item_sk_node_12, ws_bill_customer_sk_node_12, ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk_node_12, ws_bill_addr_sk_node_12, ws_ship_customer_sk_node_12, ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk_node_12, ws_ship_addr_sk_node_12, ws_web_page_sk_node_12, ws_web_site_sk_node_12, ws_ship_mode_sk_node_12, ws_warehouse_sk_node_12, ws_promo_sk_node_12, ws_order_number_node_12, ws_quantity_node_12, ws_wholesale_cost_node_12, ws_list_price_node_12, ws_sales_price_node_12, ws_ext_discount_amt_node_12, ws_ext_sales_price_node_12, ws_ext_wholesale_cost_node_12, ws_ext_list_price_node_12, ws_ext_tax_node_12, ws_coupon_amt_node_12, ws_ext_ship_cost_node_12, ws_net_paid_node_12, ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax_node_12, ws_net_profit_node_12, ws_ship_hdemo_sk_node_120], isBroadcast=[true], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- Calc(select=[EXPR$0 AS sr_addr_sk_node_11])
                        :     +- HashAggregate(isMerge=[true], groupBy=[sr_return_tax], select=[sr_return_tax, Final_COUNT(count$0) AS EXPR$0])
                        :        +- Exchange(distribution=[hash[sr_return_tax]])
                        :           +- LocalHashAggregate(groupBy=[sr_return_tax], select=[sr_return_tax, Partial_COUNT(sr_addr_sk) AS count$0])
                        :              +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_addr_sk, sr_return_tax], metadata=[]]], fields=[sr_addr_sk, sr_return_tax])
                        +- Calc(select=[ws_sold_date_sk AS ws_sold_date_sk_node_12, ws_sold_time_sk AS ws_sold_time_sk_node_12, ws_ship_date_sk AS ws_ship_date_sk_node_12, ws_item_sk AS ws_item_sk_node_12, ws_bill_customer_sk AS ws_bill_customer_sk_node_12, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_12, ws_bill_addr_sk AS ws_bill_addr_sk_node_12, ws_ship_customer_sk AS ws_ship_customer_sk_node_12, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_12, ws_ship_addr_sk AS ws_ship_addr_sk_node_12, ws_web_page_sk AS ws_web_page_sk_node_12, ws_web_site_sk AS ws_web_site_sk_node_12, ws_ship_mode_sk AS ws_ship_mode_sk_node_12, ws_warehouse_sk AS ws_warehouse_sk_node_12, ws_promo_sk AS ws_promo_sk_node_12, ws_order_number AS ws_order_number_node_12, ws_quantity AS ws_quantity_node_12, ws_wholesale_cost AS ws_wholesale_cost_node_12, ws_list_price AS ws_list_price_node_12, ws_sales_price AS ws_sales_price_node_12, ws_ext_discount_amt AS ws_ext_discount_amt_node_12, ws_ext_sales_price AS ws_ext_sales_price_node_12, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_12, ws_ext_list_price AS ws_ext_list_price_node_12, ws_ext_tax AS ws_ext_tax_node_12, ws_coupon_amt AS ws_coupon_amt_node_12, ws_ext_ship_cost AS ws_ext_ship_cost_node_12, ws_net_paid AS ws_net_paid_node_12, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_12, ws_net_profit AS ws_net_profit_node_12, CAST(ws_ship_hdemo_sk AS BIGINT) AS ws_ship_hdemo_sk_node_120], where=[f0])
                           +- PythonCalc(select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(ws_ship_hdemo_sk) AS f0])
                              +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[40], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[40], global=[false])
      +- Calc(select=[EXPR$0], where=[(EXPR$0 <= 1.897379159927368E1)])
         +- HashAggregate(isMerge=[false], groupBy=[ws_sold_date_sk_node_12], select=[ws_sold_date_sk_node_12, MIN(ws_net_paid_inc_ship_tax_node_12) AS EXPR$0])
            +- Exchange(distribution=[hash[ws_sold_date_sk_node_12]])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[(ws_ship_addr_sk_node_12 = cs_order_number_node_10)], select=[cs_sold_date_sk_node_10, cs_sold_time_sk_node_10, cs_ship_date_sk_node_10, cs_bill_customer_sk_node_10, cs_bill_cdemo_sk_node_10, cs_bill_hdemo_sk_node_10, cs_bill_addr_sk_node_10, cs_ship_customer_sk_node_10, cs_ship_cdemo_sk_node_10, cs_ship_hdemo_sk_node_10, cs_ship_addr_sk_node_10, cs_call_center_sk_node_10, cs_catalog_page_sk_node_10, cs_ship_mode_sk_node_10, cs_warehouse_sk_node_10, cs_item_sk_node_10, cs_promo_sk_node_10, cs_order_number_node_10, cs_quantity_node_10, cs_wholesale_cost_node_10, cs_list_price_node_10, cs_sales_price_node_10, cs_ext_discount_amt_node_10, cs_ext_sales_price_node_10, cs_ext_wholesale_cost_node_10, cs_ext_list_price_node_10, cs_ext_tax_node_10, cs_coupon_amt_node_10, cs_ext_ship_cost_node_10, cs_net_paid_node_10, cs_net_paid_inc_tax_node_10, cs_net_paid_inc_ship_node_10, cs_net_paid_inc_ship_tax_node_10, cs_net_profit_node_10, _c34, sr_addr_sk_node_11, ws_sold_date_sk_node_12, ws_sold_time_sk_node_12, ws_ship_date_sk_node_12, ws_item_sk_node_12, ws_bill_customer_sk_node_12, ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk_node_12, ws_bill_addr_sk_node_12, ws_ship_customer_sk_node_12, ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk_node_12, ws_ship_addr_sk_node_12, ws_web_page_sk_node_12, ws_web_site_sk_node_12, ws_ship_mode_sk_node_12, ws_warehouse_sk_node_12, ws_promo_sk_node_12, ws_order_number_node_12, ws_quantity_node_12, ws_wholesale_cost_node_12, ws_list_price_node_12, ws_sales_price_node_12, ws_ext_discount_amt_node_12, ws_ext_sales_price_node_12, ws_ext_wholesale_cost_node_12, ws_ext_list_price_node_12, ws_ext_tax_node_12, ws_coupon_amt_node_12, ws_ext_ship_cost_node_12, ws_net_paid_node_12, ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax_node_12, ws_net_profit_node_12], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[cs_sold_date_sk AS cs_sold_date_sk_node_10, cs_sold_time_sk AS cs_sold_time_sk_node_10, cs_ship_date_sk AS cs_ship_date_sk_node_10, cs_bill_customer_sk AS cs_bill_customer_sk_node_10, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_10, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_10, cs_bill_addr_sk AS cs_bill_addr_sk_node_10, cs_ship_customer_sk AS cs_ship_customer_sk_node_10, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_10, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_10, cs_ship_addr_sk AS cs_ship_addr_sk_node_10, cs_call_center_sk AS cs_call_center_sk_node_10, cs_catalog_page_sk AS cs_catalog_page_sk_node_10, cs_ship_mode_sk AS cs_ship_mode_sk_node_10, cs_warehouse_sk AS cs_warehouse_sk_node_10, cs_item_sk AS cs_item_sk_node_10, cs_promo_sk AS cs_promo_sk_node_10, cs_order_number AS cs_order_number_node_10, cs_quantity AS cs_quantity_node_10, cs_wholesale_cost AS cs_wholesale_cost_node_10, cs_list_price AS cs_list_price_node_10, cs_sales_price AS cs_sales_price_node_10, cs_ext_discount_amt AS cs_ext_discount_amt_node_10, cs_ext_sales_price AS cs_ext_sales_price_node_10, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_10, cs_ext_list_price AS cs_ext_list_price_node_10, cs_ext_tax AS cs_ext_tax_node_10, cs_coupon_amt AS cs_coupon_amt_node_10, cs_ext_ship_cost AS cs_ext_ship_cost_node_10, cs_net_paid AS cs_net_paid_node_10, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_10, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_10, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_10, cs_net_profit AS cs_net_profit_node_10, 'hello' AS _c34], where=[(cs_ext_wholesale_cost < -1.7767274379730225E1)])
                  :     +- SortLimit(orderBy=[cs_ext_tax ASC], offset=[0], fetch=[1], global=[true])
                  :        +- Exchange(distribution=[single])
                  :           +- SortLimit(orderBy=[cs_ext_tax ASC], offset=[0], fetch=[1], global=[false])
                  :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
                  +- Calc(select=[sr_addr_sk_node_11, ws_sold_date_sk_node_12, ws_sold_time_sk_node_12, ws_ship_date_sk_node_12, ws_item_sk_node_12, ws_bill_customer_sk_node_12, ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk_node_12, ws_bill_addr_sk_node_12, ws_ship_customer_sk_node_12, ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk_node_12, ws_ship_addr_sk_node_12, ws_web_page_sk_node_12, ws_web_site_sk_node_12, ws_ship_mode_sk_node_12, ws_warehouse_sk_node_12, ws_promo_sk_node_12, ws_order_number_node_12, ws_quantity_node_12, ws_wholesale_cost_node_12, ws_list_price_node_12, ws_sales_price_node_12, ws_ext_discount_amt_node_12, ws_ext_sales_price_node_12, ws_ext_wholesale_cost_node_12, ws_ext_list_price_node_12, ws_ext_tax_node_12, ws_coupon_amt_node_12, ws_ext_ship_cost_node_12, ws_net_paid_node_12, ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax_node_12, ws_net_profit_node_12])
                     +- HashJoin(joinType=[InnerJoin], where=[(sr_addr_sk_node_11 = ws_ship_hdemo_sk_node_120)], select=[sr_addr_sk_node_11, ws_sold_date_sk_node_12, ws_sold_time_sk_node_12, ws_ship_date_sk_node_12, ws_item_sk_node_12, ws_bill_customer_sk_node_12, ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk_node_12, ws_bill_addr_sk_node_12, ws_ship_customer_sk_node_12, ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk_node_12, ws_ship_addr_sk_node_12, ws_web_page_sk_node_12, ws_web_site_sk_node_12, ws_ship_mode_sk_node_12, ws_warehouse_sk_node_12, ws_promo_sk_node_12, ws_order_number_node_12, ws_quantity_node_12, ws_wholesale_cost_node_12, ws_list_price_node_12, ws_sales_price_node_12, ws_ext_discount_amt_node_12, ws_ext_sales_price_node_12, ws_ext_wholesale_cost_node_12, ws_ext_list_price_node_12, ws_ext_tax_node_12, ws_coupon_amt_node_12, ws_ext_ship_cost_node_12, ws_net_paid_node_12, ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax_node_12, ws_net_profit_node_12, ws_ship_hdemo_sk_node_120], isBroadcast=[true], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- Calc(select=[EXPR$0 AS sr_addr_sk_node_11])
                        :     +- HashAggregate(isMerge=[true], groupBy=[sr_return_tax], select=[sr_return_tax, Final_COUNT(count$0) AS EXPR$0])
                        :        +- Exchange(distribution=[hash[sr_return_tax]])
                        :           +- LocalHashAggregate(groupBy=[sr_return_tax], select=[sr_return_tax, Partial_COUNT(sr_addr_sk) AS count$0])
                        :              +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_addr_sk, sr_return_tax], metadata=[]]], fields=[sr_addr_sk, sr_return_tax])
                        +- Calc(select=[ws_sold_date_sk AS ws_sold_date_sk_node_12, ws_sold_time_sk AS ws_sold_time_sk_node_12, ws_ship_date_sk AS ws_ship_date_sk_node_12, ws_item_sk AS ws_item_sk_node_12, ws_bill_customer_sk AS ws_bill_customer_sk_node_12, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_12, ws_bill_addr_sk AS ws_bill_addr_sk_node_12, ws_ship_customer_sk AS ws_ship_customer_sk_node_12, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_12, ws_ship_addr_sk AS ws_ship_addr_sk_node_12, ws_web_page_sk AS ws_web_page_sk_node_12, ws_web_site_sk AS ws_web_site_sk_node_12, ws_ship_mode_sk AS ws_ship_mode_sk_node_12, ws_warehouse_sk AS ws_warehouse_sk_node_12, ws_promo_sk AS ws_promo_sk_node_12, ws_order_number AS ws_order_number_node_12, ws_quantity AS ws_quantity_node_12, ws_wholesale_cost AS ws_wholesale_cost_node_12, ws_list_price AS ws_list_price_node_12, ws_sales_price AS ws_sales_price_node_12, ws_ext_discount_amt AS ws_ext_discount_amt_node_12, ws_ext_sales_price AS ws_ext_sales_price_node_12, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_12, ws_ext_list_price AS ws_ext_list_price_node_12, ws_ext_tax AS ws_ext_tax_node_12, ws_coupon_amt AS ws_coupon_amt_node_12, ws_ext_ship_cost AS ws_ext_ship_cost_node_12, ws_net_paid AS ws_net_paid_node_12, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_12, ws_net_profit AS ws_net_profit_node_12, CAST(ws_ship_hdemo_sk AS BIGINT) AS ws_ship_hdemo_sk_node_120], where=[f0])
                           +- PythonCalc(select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(ws_ship_hdemo_sk) AS f0])
                              +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0