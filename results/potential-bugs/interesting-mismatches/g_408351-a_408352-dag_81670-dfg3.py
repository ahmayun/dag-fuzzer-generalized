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
    return values.iloc[-1] if len(values) > 0 else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_12 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_11 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_14 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_9 = autonode_12.join(autonode_13, col('cr_refunded_customer_sk_node_12') == col('ws_bill_customer_sk_node_13'))
autonode_8 = autonode_11.order_by(col('cp_end_date_sk_node_11'))
autonode_10 = autonode_14.alias('7A8rW')
autonode_6 = autonode_9.order_by(col('cr_return_amt_inc_tax_node_12'))
autonode_5 = autonode_8.alias('oHQoz')
autonode_7 = autonode_10.order_by(col('sr_net_loss_node_14'))
autonode_3 = autonode_5.join(autonode_6, col('cp_start_date_sk_node_11') == col('ws_order_number_node_13'))
autonode_4 = autonode_7.add_columns(lit("hello"))
autonode_2 = autonode_3.join(autonode_4, col('cr_refunded_customer_sk_node_12') == col('sr_reason_sk_node_14'))
autonode_1 = autonode_2.group_by(col('cr_catalog_page_sk_node_12')).select(col('cr_returning_addr_sk_node_12').max.alias('cr_returning_addr_sk_node_12'))
sink = autonode_1.limit(66)
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
      "error_message": "An error occurred while calling o222198063.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#449953014:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[19](input=RelSubset#449953012,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[19]), rel#449953011:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[19](input=RelSubset#449953010,groupBy=sr_reason_sk,select=sr_reason_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (19) must be less than size (1)
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
LogicalSort(fetch=[66])
+- LogicalProject(cr_returning_addr_sk_node_12=[$1])
   +- LogicalAggregate(group=[{21}], EXPR$0=[MAX($19)])
      +- LogicalJoin(condition=[=($12, $78)], joinType=[inner])
         :- LogicalJoin(condition=[=($2, $53)], joinType=[inner])
         :  :- LogicalProject(oHQoz=[AS($0, _UTF-16LE'oHQoz')], cp_catalog_page_id_node_11=[$1], cp_start_date_sk_node_11=[$2], cp_end_date_sk_node_11=[$3], cp_department_node_11=[$4], cp_catalog_number_node_11=[$5], cp_catalog_page_number_node_11=[$6], cp_description_node_11=[$7], cp_type_node_11=[$8])
         :  :  +- LogicalSort(sort0=[$3], dir0=[ASC])
         :  :     +- LogicalProject(cp_catalog_page_sk_node_11=[$0], cp_catalog_page_id_node_11=[$1], cp_start_date_sk_node_11=[$2], cp_end_date_sk_node_11=[$3], cp_department_node_11=[$4], cp_catalog_number_node_11=[$5], cp_catalog_page_number_node_11=[$6], cp_description_node_11=[$7], cp_type_node_11=[$8])
         :  :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])
         :  +- LogicalSort(sort0=[$20], dir0=[ASC])
         :     +- LogicalJoin(condition=[=($3, $31)], joinType=[inner])
         :        :- LogicalProject(cr_returned_date_sk_node_12=[$0], cr_returned_time_sk_node_12=[$1], cr_item_sk_node_12=[$2], cr_refunded_customer_sk_node_12=[$3], cr_refunded_cdemo_sk_node_12=[$4], cr_refunded_hdemo_sk_node_12=[$5], cr_refunded_addr_sk_node_12=[$6], cr_returning_customer_sk_node_12=[$7], cr_returning_cdemo_sk_node_12=[$8], cr_returning_hdemo_sk_node_12=[$9], cr_returning_addr_sk_node_12=[$10], cr_call_center_sk_node_12=[$11], cr_catalog_page_sk_node_12=[$12], cr_ship_mode_sk_node_12=[$13], cr_warehouse_sk_node_12=[$14], cr_reason_sk_node_12=[$15], cr_order_number_node_12=[$16], cr_return_quantity_node_12=[$17], cr_return_amount_node_12=[$18], cr_return_tax_node_12=[$19], cr_return_amt_inc_tax_node_12=[$20], cr_fee_node_12=[$21], cr_return_ship_cost_node_12=[$22], cr_refunded_cash_node_12=[$23], cr_reversed_charge_node_12=[$24], cr_store_credit_node_12=[$25], cr_net_loss_node_12=[$26])
         :        :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
         :        +- LogicalProject(ws_sold_date_sk_node_13=[$0], ws_sold_time_sk_node_13=[$1], ws_ship_date_sk_node_13=[$2], ws_item_sk_node_13=[$3], ws_bill_customer_sk_node_13=[$4], ws_bill_cdemo_sk_node_13=[$5], ws_bill_hdemo_sk_node_13=[$6], ws_bill_addr_sk_node_13=[$7], ws_ship_customer_sk_node_13=[$8], ws_ship_cdemo_sk_node_13=[$9], ws_ship_hdemo_sk_node_13=[$10], ws_ship_addr_sk_node_13=[$11], ws_web_page_sk_node_13=[$12], ws_web_site_sk_node_13=[$13], ws_ship_mode_sk_node_13=[$14], ws_warehouse_sk_node_13=[$15], ws_promo_sk_node_13=[$16], ws_order_number_node_13=[$17], ws_quantity_node_13=[$18], ws_wholesale_cost_node_13=[$19], ws_list_price_node_13=[$20], ws_sales_price_node_13=[$21], ws_ext_discount_amt_node_13=[$22], ws_ext_sales_price_node_13=[$23], ws_ext_wholesale_cost_node_13=[$24], ws_ext_list_price_node_13=[$25], ws_ext_tax_node_13=[$26], ws_coupon_amt_node_13=[$27], ws_ext_ship_cost_node_13=[$28], ws_net_paid_node_13=[$29], ws_net_paid_inc_tax_node_13=[$30], ws_net_paid_inc_ship_node_13=[$31], ws_net_paid_inc_ship_tax_node_13=[$32], ws_net_profit_node_13=[$33])
         :           +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
         +- LogicalProject(7A8rW=[$0], sr_return_time_sk_node_14=[$1], sr_item_sk_node_14=[$2], sr_customer_sk_node_14=[$3], sr_cdemo_sk_node_14=[$4], sr_hdemo_sk_node_14=[$5], sr_addr_sk_node_14=[$6], sr_store_sk_node_14=[$7], sr_reason_sk_node_14=[$8], sr_ticket_number_node_14=[$9], sr_return_quantity_node_14=[$10], sr_return_amt_node_14=[$11], sr_return_tax_node_14=[$12], sr_return_amt_inc_tax_node_14=[$13], sr_fee_node_14=[$14], sr_return_ship_cost_node_14=[$15], sr_refunded_cash_node_14=[$16], sr_reversed_charge_node_14=[$17], sr_store_credit_node_14=[$18], sr_net_loss_node_14=[$19], _c20=[_UTF-16LE'hello'])
            +- LogicalSort(sort0=[$19], dir0=[ASC])
               +- LogicalProject(7A8rW=[AS($0, _UTF-16LE'7A8rW')], sr_return_time_sk_node_14=[$1], sr_item_sk_node_14=[$2], sr_customer_sk_node_14=[$3], sr_cdemo_sk_node_14=[$4], sr_hdemo_sk_node_14=[$5], sr_addr_sk_node_14=[$6], sr_store_sk_node_14=[$7], sr_reason_sk_node_14=[$8], sr_ticket_number_node_14=[$9], sr_return_quantity_node_14=[$10], sr_return_amt_node_14=[$11], sr_return_tax_node_14=[$12], sr_return_amt_inc_tax_node_14=[$13], sr_fee_node_14=[$14], sr_return_ship_cost_node_14=[$15], sr_refunded_cash_node_14=[$16], sr_reversed_charge_node_14=[$17], sr_store_credit_node_14=[$18], sr_net_loss_node_14=[$19])
                  +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[66], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[66], global=[false])
      +- Calc(select=[EXPR$0 AS cr_returning_addr_sk_node_12])
         +- SortAggregate(isMerge=[false], groupBy=[cr_catalog_page_sk], select=[cr_catalog_page_sk, MAX(cr_returning_addr_sk) AS EXPR$0])
            +- Sort(orderBy=[cr_catalog_page_sk ASC])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cr_refunded_customer_sk, sr_reason_sk_node_14)], select=[oHQoz, cp_catalog_page_id_node_11, cp_start_date_sk_node_11, cp_end_date_sk_node_11, cp_department_node_11, cp_catalog_number_node_11, cp_catalog_page_number_node_11, cp_description_node_11, cp_type_node_11, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, 7A8rW, sr_return_time_sk_node_14, sr_item_sk_node_14, sr_customer_sk_node_14, sr_cdemo_sk_node_14, sr_hdemo_sk_node_14, sr_addr_sk_node_14, sr_store_sk_node_14, sr_reason_sk_node_14, sr_ticket_number_node_14, sr_return_quantity_node_14, sr_return_amt_node_14, sr_return_tax_node_14, sr_return_amt_inc_tax_node_14, sr_fee_node_14, sr_return_ship_cost_node_14, sr_refunded_cash_node_14, sr_reversed_charge_node_14, sr_store_credit_node_14, sr_net_loss_node_14, _c20], build=[right])
                  :- NestedLoopJoin(joinType=[InnerJoin], where=[=(cp_start_date_sk_node_11, ws_order_number)], select=[oHQoz, cp_catalog_page_id_node_11, cp_start_date_sk_node_11, cp_end_date_sk_node_11, cp_department_node_11, cp_catalog_number_node_11, cp_catalog_page_number_node_11, cp_description_node_11, cp_type_node_11, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
                  :  :- Exchange(distribution=[broadcast])
                  :  :  +- Calc(select=[cp_catalog_page_sk AS oHQoz, cp_catalog_page_id AS cp_catalog_page_id_node_11, cp_start_date_sk AS cp_start_date_sk_node_11, cp_end_date_sk AS cp_end_date_sk_node_11, cp_department AS cp_department_node_11, cp_catalog_number AS cp_catalog_number_node_11, cp_catalog_page_number AS cp_catalog_page_number_node_11, cp_description AS cp_description_node_11, cp_type AS cp_type_node_11])
                  :  :     +- SortLimit(orderBy=[cp_end_date_sk ASC], offset=[0], fetch=[1], global=[true])
                  :  :        +- Exchange(distribution=[single])
                  :  :           +- SortLimit(orderBy=[cp_end_date_sk ASC], offset=[0], fetch=[1], global=[false])
                  :  :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
                  :  +- Exchange(distribution=[hash[cr_catalog_page_sk]])
                  :     +- SortLimit(orderBy=[cr_return_amt_inc_tax ASC], offset=[0], fetch=[1], global=[true])
                  :        +- Exchange(distribution=[single])
                  :           +- SortLimit(orderBy=[cr_return_amt_inc_tax ASC], offset=[0], fetch=[1], global=[false])
                  :              +- HashJoin(joinType=[InnerJoin], where=[=(cr_refunded_customer_sk, ws_bill_customer_sk)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
                  :                 :- Exchange(distribution=[hash[cr_refunded_customer_sk]])
                  :                 :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                  :                 +- Exchange(distribution=[hash[ws_bill_customer_sk]])
                  :                    +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                  +- Exchange(distribution=[broadcast])
                     +- Calc(select=[sr_returned_date_sk AS 7A8rW, sr_return_time_sk AS sr_return_time_sk_node_14, sr_item_sk AS sr_item_sk_node_14, sr_customer_sk AS sr_customer_sk_node_14, sr_cdemo_sk AS sr_cdemo_sk_node_14, sr_hdemo_sk AS sr_hdemo_sk_node_14, sr_addr_sk AS sr_addr_sk_node_14, sr_store_sk AS sr_store_sk_node_14, sr_reason_sk AS sr_reason_sk_node_14, sr_ticket_number AS sr_ticket_number_node_14, sr_return_quantity AS sr_return_quantity_node_14, sr_return_amt AS sr_return_amt_node_14, sr_return_tax AS sr_return_tax_node_14, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_14, sr_fee AS sr_fee_node_14, sr_return_ship_cost AS sr_return_ship_cost_node_14, sr_refunded_cash AS sr_refunded_cash_node_14, sr_reversed_charge AS sr_reversed_charge_node_14, sr_store_credit AS sr_store_credit_node_14, sr_net_loss AS sr_net_loss_node_14, 'hello' AS _c20])
                        +- SortLimit(orderBy=[sr_net_loss ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[sr_net_loss ASC], offset=[0], fetch=[1], global=[false])
                                 +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[66], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[66], global=[false])
      +- Calc(select=[EXPR$0 AS cr_returning_addr_sk_node_12])
         +- SortAggregate(isMerge=[false], groupBy=[cr_catalog_page_sk], select=[cr_catalog_page_sk, MAX(cr_returning_addr_sk) AS EXPR$0])
            +- Exchange(distribution=[forward])
               +- Sort(orderBy=[cr_catalog_page_sk ASC])
                  +- Exchange(distribution=[keep_input_as_is[hash[cr_catalog_page_sk]]])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(cr_refunded_customer_sk = sr_reason_sk_node_14)], select=[oHQoz, cp_catalog_page_id_node_11, cp_start_date_sk_node_11, cp_end_date_sk_node_11, cp_department_node_11, cp_catalog_number_node_11, cp_catalog_page_number_node_11, cp_description_node_11, cp_type_node_11, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, 7A8rW, sr_return_time_sk_node_14, sr_item_sk_node_14, sr_customer_sk_node_14, sr_cdemo_sk_node_14, sr_hdemo_sk_node_14, sr_addr_sk_node_14, sr_store_sk_node_14, sr_reason_sk_node_14, sr_ticket_number_node_14, sr_return_quantity_node_14, sr_return_amt_node_14, sr_return_tax_node_14, sr_return_amt_inc_tax_node_14, sr_fee_node_14, sr_return_ship_cost_node_14, sr_refunded_cash_node_14, sr_reversed_charge_node_14, sr_store_credit_node_14, sr_net_loss_node_14, _c20], build=[right])
                        :- NestedLoopJoin(joinType=[InnerJoin], where=[(cp_start_date_sk_node_11 = ws_order_number)], select=[oHQoz, cp_catalog_page_id_node_11, cp_start_date_sk_node_11, cp_end_date_sk_node_11, cp_department_node_11, cp_catalog_number_node_11, cp_catalog_page_number_node_11, cp_description_node_11, cp_type_node_11, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
                        :  :- Exchange(distribution=[broadcast])
                        :  :  +- Calc(select=[cp_catalog_page_sk AS oHQoz, cp_catalog_page_id AS cp_catalog_page_id_node_11, cp_start_date_sk AS cp_start_date_sk_node_11, cp_end_date_sk AS cp_end_date_sk_node_11, cp_department AS cp_department_node_11, cp_catalog_number AS cp_catalog_number_node_11, cp_catalog_page_number AS cp_catalog_page_number_node_11, cp_description AS cp_description_node_11, cp_type AS cp_type_node_11])
                        :  :     +- SortLimit(orderBy=[cp_end_date_sk ASC], offset=[0], fetch=[1], global=[true])
                        :  :        +- Exchange(distribution=[single])
                        :  :           +- SortLimit(orderBy=[cp_end_date_sk ASC], offset=[0], fetch=[1], global=[false])
                        :  :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
                        :  +- Exchange(distribution=[hash[cr_catalog_page_sk]])
                        :     +- SortLimit(orderBy=[cr_return_amt_inc_tax ASC], offset=[0], fetch=[1], global=[true])
                        :        +- Exchange(distribution=[single])
                        :           +- SortLimit(orderBy=[cr_return_amt_inc_tax ASC], offset=[0], fetch=[1], global=[false])
                        :              +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(cr_refunded_customer_sk = ws_bill_customer_sk)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
                        :                 :- Exchange(distribution=[hash[cr_refunded_customer_sk]])
                        :                 :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                        :                 +- Exchange(distribution=[hash[ws_bill_customer_sk]])
                        :                    +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[sr_returned_date_sk AS 7A8rW, sr_return_time_sk AS sr_return_time_sk_node_14, sr_item_sk AS sr_item_sk_node_14, sr_customer_sk AS sr_customer_sk_node_14, sr_cdemo_sk AS sr_cdemo_sk_node_14, sr_hdemo_sk AS sr_hdemo_sk_node_14, sr_addr_sk AS sr_addr_sk_node_14, sr_store_sk AS sr_store_sk_node_14, sr_reason_sk AS sr_reason_sk_node_14, sr_ticket_number AS sr_ticket_number_node_14, sr_return_quantity AS sr_return_quantity_node_14, sr_return_amt AS sr_return_amt_node_14, sr_return_tax AS sr_return_tax_node_14, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_14, sr_fee AS sr_fee_node_14, sr_return_ship_cost AS sr_return_ship_cost_node_14, sr_refunded_cash AS sr_refunded_cash_node_14, sr_reversed_charge AS sr_reversed_charge_node_14, sr_store_credit AS sr_store_credit_node_14, sr_net_loss AS sr_net_loss_node_14, 'hello' AS _c20])
                              +- SortLimit(orderBy=[sr_net_loss ASC], offset=[0], fetch=[1], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- SortLimit(orderBy=[sr_net_loss ASC], offset=[0], fetch=[1], global=[false])
                                       +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0