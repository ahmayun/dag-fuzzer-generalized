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


def preloaded_aggregation(values: pd.Series) -> int:
    return values.count()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_9 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_11 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_7 = autonode_9.distinct()
autonode_8 = autonode_10.join(autonode_11, col('wp_image_count_node_10') == col('cs_item_sk_node_11'))
autonode_5 = autonode_7.order_by(col('hd_income_band_sk_node_9'))
autonode_6 = autonode_8.limit(59)
autonode_4 = autonode_5.join(autonode_6, col('hd_income_band_sk_node_9') == col('cs_ship_customer_sk_node_11'))
autonode_3 = autonode_4.group_by(col('cs_ship_customer_sk_node_11')).select(col('cs_coupon_amt_node_11').min.alias('cs_coupon_amt_node_11'))
autonode_2 = autonode_3.group_by(col('cs_coupon_amt_node_11')).select(call('preloaded_udf_agg', col('cs_coupon_amt_node_11')).alias('cs_coupon_amt_node_11'))
autonode_1 = autonode_2.filter(preloaded_udf_boolean(col('cs_coupon_amt_node_11')))
sink = autonode_1.group_by(col('cs_coupon_amt_node_11')).select(col('cs_coupon_amt_node_11').count.alias('cs_coupon_amt_node_11'))
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
      "error_message": "An error occurred while calling o343015269.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#691137095:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[1](input=RelSubset#691137093,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[1]), rel#691137092:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[1](input=RelSubset#691137091,groupBy=hd_income_band_sk,select=hd_income_band_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (1) must be less than size (1)
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
LogicalProject(cs_coupon_amt_node_11=[$1])
+- LogicalAggregate(group=[{0}], EXPR$0=[COUNT($0)])
   +- LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($0)])
      +- LogicalProject(cs_coupon_amt_node_11=[$1])
         +- LogicalAggregate(group=[{0}], EXPR$0=[preloaded_udf_agg($0)])
            +- LogicalProject(cs_coupon_amt_node_11=[$1])
               +- LogicalAggregate(group=[{26}], EXPR$0=[MIN($46)])
                  +- LogicalJoin(condition=[=($1, $26)], joinType=[inner])
                     :- LogicalSort(sort0=[$1], dir0=[ASC])
                     :  +- LogicalAggregate(group=[{0, 1, 2, 3, 4}])
                     :     +- LogicalProject(hd_demo_sk_node_9=[$0], hd_income_band_sk_node_9=[$1], hd_buy_potential_node_9=[$2], hd_dep_count_node_9=[$3], hd_vehicle_count_node_9=[$4])
                     :        +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
                     +- LogicalSort(fetch=[59])
                        +- LogicalJoin(condition=[=($12, $29)], joinType=[inner])
                           :- LogicalProject(wp_web_page_sk_node_10=[$0], wp_web_page_id_node_10=[$1], wp_rec_start_date_node_10=[$2], wp_rec_end_date_node_10=[$3], wp_creation_date_sk_node_10=[$4], wp_access_date_sk_node_10=[$5], wp_autogen_flag_node_10=[$6], wp_customer_sk_node_10=[$7], wp_url_node_10=[$8], wp_type_node_10=[$9], wp_char_count_node_10=[$10], wp_link_count_node_10=[$11], wp_image_count_node_10=[$12], wp_max_ad_count_node_10=[$13])
                           :  +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
                           +- LogicalProject(cs_sold_date_sk_node_11=[$0], cs_sold_time_sk_node_11=[$1], cs_ship_date_sk_node_11=[$2], cs_bill_customer_sk_node_11=[$3], cs_bill_cdemo_sk_node_11=[$4], cs_bill_hdemo_sk_node_11=[$5], cs_bill_addr_sk_node_11=[$6], cs_ship_customer_sk_node_11=[$7], cs_ship_cdemo_sk_node_11=[$8], cs_ship_hdemo_sk_node_11=[$9], cs_ship_addr_sk_node_11=[$10], cs_call_center_sk_node_11=[$11], cs_catalog_page_sk_node_11=[$12], cs_ship_mode_sk_node_11=[$13], cs_warehouse_sk_node_11=[$14], cs_item_sk_node_11=[$15], cs_promo_sk_node_11=[$16], cs_order_number_node_11=[$17], cs_quantity_node_11=[$18], cs_wholesale_cost_node_11=[$19], cs_list_price_node_11=[$20], cs_sales_price_node_11=[$21], cs_ext_discount_amt_node_11=[$22], cs_ext_sales_price_node_11=[$23], cs_ext_wholesale_cost_node_11=[$24], cs_ext_list_price_node_11=[$25], cs_ext_tax_node_11=[$26], cs_coupon_amt_node_11=[$27], cs_ext_ship_cost_node_11=[$28], cs_net_paid_node_11=[$29], cs_net_paid_inc_tax_node_11=[$30], cs_net_paid_inc_ship_node_11=[$31], cs_net_paid_inc_ship_tax_node_11=[$32], cs_net_profit_node_11=[$33])
                              +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0_0 AS cs_coupon_amt_node_11])
+- SortAggregate(isMerge=[false], groupBy=[EXPR$0], select=[EXPR$0, COUNT(EXPR$0) AS EXPR$0_0])
   +- Sort(orderBy=[EXPR$0 ASC])
      +- Exchange(distribution=[hash[EXPR$0]])
         +- Calc(select=[EXPR$0], where=[f0])
            +- PythonCalc(select=[EXPR$0, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(EXPR$0) AS f0])
               +- PythonGroupAggregate(groupBy=[cs_coupon_amt_node_11], select=[cs_coupon_amt_node_11, preloaded_udf_agg(cs_coupon_amt_node_11) AS EXPR$0])
                  +- Sort(orderBy=[cs_coupon_amt_node_11 ASC])
                     +- Exchange(distribution=[hash[cs_coupon_amt_node_11]])
                        +- Calc(select=[EXPR$0 AS cs_coupon_amt_node_11])
                           +- HashAggregate(isMerge=[false], groupBy=[cs_ship_customer_sk], select=[cs_ship_customer_sk, MIN(cs_coupon_amt) AS EXPR$0])
                              +- Exchange(distribution=[hash[cs_ship_customer_sk]])
                                 +- NestedLoopJoin(joinType=[InnerJoin], where=[=(hd_income_band_sk, cs_ship_customer_sk)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], build=[left])
                                    :- Exchange(distribution=[broadcast])
                                    :  +- SortLimit(orderBy=[hd_income_band_sk ASC], offset=[0], fetch=[1], global=[true])
                                    :     +- Exchange(distribution=[single])
                                    :        +- SortLimit(orderBy=[hd_income_band_sk ASC], offset=[0], fetch=[1], global=[false])
                                    :           +- HashAggregate(isMerge=[true], groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                    :              +- Exchange(distribution=[hash[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count]])
                                    :                 +- LocalHashAggregate(groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                    :                    +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                    +- Limit(offset=[0], fetch=[59], global=[true])
                                       +- Exchange(distribution=[single])
                                          +- Limit(offset=[0], fetch=[59], global=[false])
                                             +- HashJoin(joinType=[InnerJoin], where=[=(wp_image_count, cs_item_sk)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], isBroadcast=[true], build=[left])
                                                :- Exchange(distribution=[broadcast])
                                                :  +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                                                +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0_0 AS cs_coupon_amt_node_11])
+- SortAggregate(isMerge=[false], groupBy=[EXPR$0], select=[EXPR$0, COUNT(EXPR$0) AS EXPR$0_0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[EXPR$0 ASC])
         +- Exchange(distribution=[hash[EXPR$0]])
            +- Calc(select=[EXPR$0], where=[f0])
               +- PythonCalc(select=[EXPR$0, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(EXPR$0) AS f0])
                  +- PythonGroupAggregate(groupBy=[cs_coupon_amt_node_11], select=[cs_coupon_amt_node_11, preloaded_udf_agg(cs_coupon_amt_node_11) AS EXPR$0])
                     +- Exchange(distribution=[keep_input_as_is[hash[cs_coupon_amt_node_11]]])
                        +- Sort(orderBy=[cs_coupon_amt_node_11 ASC])
                           +- Exchange(distribution=[hash[cs_coupon_amt_node_11]])
                              +- Calc(select=[EXPR$0 AS cs_coupon_amt_node_11])
                                 +- HashAggregate(isMerge=[false], groupBy=[cs_ship_customer_sk], select=[cs_ship_customer_sk, MIN(cs_coupon_amt) AS EXPR$0])
                                    +- Exchange(distribution=[hash[cs_ship_customer_sk]])
                                       +- NestedLoopJoin(joinType=[InnerJoin], where=[(hd_income_band_sk = cs_ship_customer_sk)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], build=[left])
                                          :- Exchange(distribution=[broadcast])
                                          :  +- SortLimit(orderBy=[hd_income_band_sk ASC], offset=[0], fetch=[1], global=[true])
                                          :     +- Exchange(distribution=[single])
                                          :        +- SortLimit(orderBy=[hd_income_band_sk ASC], offset=[0], fetch=[1], global=[false])
                                          :           +- HashAggregate(isMerge=[true], groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                          :              +- Exchange(distribution=[hash[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count]])
                                          :                 +- LocalHashAggregate(groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                          :                    +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                          +- Limit(offset=[0], fetch=[59], global=[true])
                                             +- Exchange(distribution=[single])
                                                +- Limit(offset=[0], fetch=[59], global=[false])
                                                   +- HashJoin(joinType=[InnerJoin], where=[(wp_image_count = cs_item_sk)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], isBroadcast=[true], build=[left])
                                                      :- Exchange(distribution=[broadcast])
                                                      :  +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                                                      +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0