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

autonode_6 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_8 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_7 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_5 = autonode_8.add_columns(lit("hello"))
autonode_4 = autonode_6.join(autonode_7, col('sr_reversed_charge_node_6') == col('cs_wholesale_cost_node_7'))
autonode_3 = autonode_5.distinct()
autonode_2 = autonode_4.order_by(col('cs_order_number_node_7'))
autonode_1 = autonode_2.join(autonode_3, col('sr_addr_sk_node_6') == col('web_close_date_sk_node_8'))
sink = autonode_1.group_by(col('web_company_id_node_8')).select(col('web_suite_number_node_8').min.alias('web_suite_number_node_8'))
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
      "error_message": "An error occurred while calling o159216910.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#321856366:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[37](input=RelSubset#321856364,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[37]), rel#321856363:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[37](input=RelSubset#321856362,groupBy=sr_addr_sk,select=sr_addr_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (37) must be less than size (1)
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
LogicalProject(web_suite_number_node_8=[$1])
+- LogicalAggregate(group=[{67}], EXPR$0=[MIN($72)])
   +- LogicalJoin(condition=[=($6, $60)], joinType=[inner])
      :- LogicalSort(sort0=[$37], dir0=[ASC])
      :  +- LogicalJoin(condition=[=($17, $39)], joinType=[inner])
      :     :- LogicalProject(sr_returned_date_sk_node_6=[$0], sr_return_time_sk_node_6=[$1], sr_item_sk_node_6=[$2], sr_customer_sk_node_6=[$3], sr_cdemo_sk_node_6=[$4], sr_hdemo_sk_node_6=[$5], sr_addr_sk_node_6=[$6], sr_store_sk_node_6=[$7], sr_reason_sk_node_6=[$8], sr_ticket_number_node_6=[$9], sr_return_quantity_node_6=[$10], sr_return_amt_node_6=[$11], sr_return_tax_node_6=[$12], sr_return_amt_inc_tax_node_6=[$13], sr_fee_node_6=[$14], sr_return_ship_cost_node_6=[$15], sr_refunded_cash_node_6=[$16], sr_reversed_charge_node_6=[$17], sr_store_credit_node_6=[$18], sr_net_loss_node_6=[$19])
      :     :  +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
      :     +- LogicalProject(cs_sold_date_sk_node_7=[$0], cs_sold_time_sk_node_7=[$1], cs_ship_date_sk_node_7=[$2], cs_bill_customer_sk_node_7=[$3], cs_bill_cdemo_sk_node_7=[$4], cs_bill_hdemo_sk_node_7=[$5], cs_bill_addr_sk_node_7=[$6], cs_ship_customer_sk_node_7=[$7], cs_ship_cdemo_sk_node_7=[$8], cs_ship_hdemo_sk_node_7=[$9], cs_ship_addr_sk_node_7=[$10], cs_call_center_sk_node_7=[$11], cs_catalog_page_sk_node_7=[$12], cs_ship_mode_sk_node_7=[$13], cs_warehouse_sk_node_7=[$14], cs_item_sk_node_7=[$15], cs_promo_sk_node_7=[$16], cs_order_number_node_7=[$17], cs_quantity_node_7=[$18], cs_wholesale_cost_node_7=[$19], cs_list_price_node_7=[$20], cs_sales_price_node_7=[$21], cs_ext_discount_amt_node_7=[$22], cs_ext_sales_price_node_7=[$23], cs_ext_wholesale_cost_node_7=[$24], cs_ext_list_price_node_7=[$25], cs_ext_tax_node_7=[$26], cs_coupon_amt_node_7=[$27], cs_ext_ship_cost_node_7=[$28], cs_net_paid_node_7=[$29], cs_net_paid_inc_tax_node_7=[$30], cs_net_paid_inc_ship_node_7=[$31], cs_net_paid_inc_ship_tax_node_7=[$32], cs_net_profit_node_7=[$33])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
      +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26}])
         +- LogicalProject(web_site_sk_node_8=[$0], web_site_id_node_8=[$1], web_rec_start_date_node_8=[$2], web_rec_end_date_node_8=[$3], web_name_node_8=[$4], web_open_date_sk_node_8=[$5], web_close_date_sk_node_8=[$6], web_class_node_8=[$7], web_manager_node_8=[$8], web_mkt_id_node_8=[$9], web_mkt_class_node_8=[$10], web_mkt_desc_node_8=[$11], web_market_manager_node_8=[$12], web_company_id_node_8=[$13], web_company_name_node_8=[$14], web_street_number_node_8=[$15], web_street_name_node_8=[$16], web_street_type_node_8=[$17], web_suite_number_node_8=[$18], web_city_node_8=[$19], web_county_node_8=[$20], web_state_node_8=[$21], web_zip_node_8=[$22], web_country_node_8=[$23], web_gmt_offset_node_8=[$24], web_tax_percentage_node_8=[$25], _c26=[_UTF-16LE'hello'])
            +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS web_suite_number_node_8])
+- SortAggregate(isMerge=[false], groupBy=[web_company_id_node_8], select=[web_company_id_node_8, MIN(web_suite_number_node_8) AS EXPR$0])
   +- Sort(orderBy=[web_company_id_node_8 ASC])
      +- Exchange(distribution=[hash[web_company_id_node_8]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(sr_addr_sk, web_close_date_sk_node_8)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, web_site_sk_node_8, web_site_id_node_8, web_rec_start_date_node_8, web_rec_end_date_node_8, web_name_node_8, web_open_date_sk_node_8, web_close_date_sk_node_8, web_class_node_8, web_manager_node_8, web_mkt_id_node_8, web_mkt_class_node_8, web_mkt_desc_node_8, web_market_manager_node_8, web_company_id_node_8, web_company_name_node_8, web_street_number_node_8, web_street_name_node_8, web_street_type_node_8, web_suite_number_node_8, web_city_node_8, web_county_node_8, web_state_node_8, web_zip_node_8, web_country_node_8, web_gmt_offset_node_8, web_tax_percentage_node_8, _c26], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- SortLimit(orderBy=[cs_order_number ASC], offset=[0], fetch=[1], global=[true])
            :     +- Exchange(distribution=[single])
            :        +- SortLimit(orderBy=[cs_order_number ASC], offset=[0], fetch=[1], global=[false])
            :           +- HashJoin(joinType=[InnerJoin], where=[=(sr_reversed_charge, cs_wholesale_cost)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], build=[left])
            :              :- Exchange(distribution=[hash[sr_reversed_charge]])
            :              :  +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
            :              +- Exchange(distribution=[hash[cs_wholesale_cost]])
            :                 +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
            +- Calc(select=[web_site_sk AS web_site_sk_node_8, web_site_id AS web_site_id_node_8, web_rec_start_date AS web_rec_start_date_node_8, web_rec_end_date AS web_rec_end_date_node_8, web_name AS web_name_node_8, web_open_date_sk AS web_open_date_sk_node_8, web_close_date_sk AS web_close_date_sk_node_8, web_class AS web_class_node_8, web_manager AS web_manager_node_8, web_mkt_id AS web_mkt_id_node_8, web_mkt_class AS web_mkt_class_node_8, web_mkt_desc AS web_mkt_desc_node_8, web_market_manager AS web_market_manager_node_8, web_company_id AS web_company_id_node_8, web_company_name AS web_company_name_node_8, web_street_number AS web_street_number_node_8, web_street_name AS web_street_name_node_8, web_street_type AS web_street_type_node_8, web_suite_number AS web_suite_number_node_8, web_city AS web_city_node_8, web_county AS web_county_node_8, web_state AS web_state_node_8, web_zip AS web_zip_node_8, web_country AS web_country_node_8, web_gmt_offset AS web_gmt_offset_node_8, web_tax_percentage AS web_tax_percentage_node_8, 'hello' AS _c26])
               +- HashAggregate(isMerge=[false], groupBy=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                  +- Exchange(distribution=[hash[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage]])
                     +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS web_suite_number_node_8])
+- SortAggregate(isMerge=[false], groupBy=[web_company_id_node_8], select=[web_company_id_node_8, MIN(web_suite_number_node_8) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[web_company_id_node_8 ASC])
         +- Exchange(distribution=[hash[web_company_id_node_8]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(sr_addr_sk = web_close_date_sk_node_8)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, web_site_sk_node_8, web_site_id_node_8, web_rec_start_date_node_8, web_rec_end_date_node_8, web_name_node_8, web_open_date_sk_node_8, web_close_date_sk_node_8, web_class_node_8, web_manager_node_8, web_mkt_id_node_8, web_mkt_class_node_8, web_mkt_desc_node_8, web_market_manager_node_8, web_company_id_node_8, web_company_name_node_8, web_street_number_node_8, web_street_name_node_8, web_street_type_node_8, web_suite_number_node_8, web_city_node_8, web_county_node_8, web_state_node_8, web_zip_node_8, web_country_node_8, web_gmt_offset_node_8, web_tax_percentage_node_8, _c26], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- SortLimit(orderBy=[cs_order_number ASC], offset=[0], fetch=[1], global=[true])
               :     +- Exchange(distribution=[single])
               :        +- SortLimit(orderBy=[cs_order_number ASC], offset=[0], fetch=[1], global=[false])
               :           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(sr_reversed_charge = cs_wholesale_cost)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], build=[left])
               :              :- Exchange(distribution=[hash[sr_reversed_charge]])
               :              :  +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
               :              +- Exchange(distribution=[hash[cs_wholesale_cost]])
               :                 +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
               +- Calc(select=[web_site_sk AS web_site_sk_node_8, web_site_id AS web_site_id_node_8, web_rec_start_date AS web_rec_start_date_node_8, web_rec_end_date AS web_rec_end_date_node_8, web_name AS web_name_node_8, web_open_date_sk AS web_open_date_sk_node_8, web_close_date_sk AS web_close_date_sk_node_8, web_class AS web_class_node_8, web_manager AS web_manager_node_8, web_mkt_id AS web_mkt_id_node_8, web_mkt_class AS web_mkt_class_node_8, web_mkt_desc AS web_mkt_desc_node_8, web_market_manager AS web_market_manager_node_8, web_company_id AS web_company_id_node_8, web_company_name AS web_company_name_node_8, web_street_number AS web_street_number_node_8, web_street_name AS web_street_name_node_8, web_street_type AS web_street_type_node_8, web_suite_number AS web_suite_number_node_8, web_city AS web_city_node_8, web_county AS web_county_node_8, web_state AS web_state_node_8, web_zip AS web_zip_node_8, web_country AS web_country_node_8, web_gmt_offset AS web_gmt_offset_node_8, web_tax_percentage AS web_tax_percentage_node_8, 'hello' AS _c26])
                  +- HashAggregate(isMerge=[false], groupBy=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                     +- Exchange(distribution=[hash[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage]])
                        +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0