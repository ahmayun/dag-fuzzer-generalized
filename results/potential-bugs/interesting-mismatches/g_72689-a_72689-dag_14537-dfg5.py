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

autonode_10 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_9 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_8 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_11 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_6 = autonode_9.join(autonode_10, col('ca_address_sk_node_10') == col('cp_catalog_number_node_9'))
autonode_5 = autonode_8.limit(34)
autonode_7 = autonode_11.order_by(col('ss_hdemo_sk_node_11'))
autonode_3 = autonode_5.join(autonode_6, col('ca_city_node_10') == col('wp_rec_start_date_node_8'))
autonode_4 = autonode_7.filter(col('ss_ext_wholesale_cost_node_11') >= -7.4744462966918945)
autonode_2 = autonode_3.join(autonode_4, col('ca_address_sk_node_10') == col('ss_ticket_number_node_11'))
autonode_1 = autonode_2.group_by(col('wp_web_page_id_node_8')).select(col('cp_type_node_9').min.alias('cp_type_node_9'))
sink = autonode_1.filter(col('cp_type_node_9').char_length > 5)
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
      "error_message": "An error occurred while calling o39704835.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#78986099:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[5](input=RelSubset#78986097,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[5]), rel#78986096:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[5](input=RelSubset#78986095,groupBy=ss_ticket_number,select=ss_ticket_number)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (5) must be less than size (1)
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
LogicalFilter(condition=[>(CHAR_LENGTH($0), 5)])
+- LogicalProject(cp_type_node_9=[$1])
   +- LogicalAggregate(group=[{1}], EXPR$0=[MIN($22)])
      +- LogicalJoin(condition=[=($23, $45)], joinType=[inner])
         :- LogicalJoin(condition=[=($29, $2)], joinType=[inner])
         :  :- LogicalSort(fetch=[34])
         :  :  +- LogicalProject(wp_web_page_sk_node_8=[$0], wp_web_page_id_node_8=[$1], wp_rec_start_date_node_8=[$2], wp_rec_end_date_node_8=[$3], wp_creation_date_sk_node_8=[$4], wp_access_date_sk_node_8=[$5], wp_autogen_flag_node_8=[$6], wp_customer_sk_node_8=[$7], wp_url_node_8=[$8], wp_type_node_8=[$9], wp_char_count_node_8=[$10], wp_link_count_node_8=[$11], wp_image_count_node_8=[$12], wp_max_ad_count_node_8=[$13])
         :  :     +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
         :  +- LogicalJoin(condition=[=($9, $5)], joinType=[inner])
         :     :- LogicalProject(cp_catalog_page_sk_node_9=[$0], cp_catalog_page_id_node_9=[$1], cp_start_date_sk_node_9=[$2], cp_end_date_sk_node_9=[$3], cp_department_node_9=[$4], cp_catalog_number_node_9=[$5], cp_catalog_page_number_node_9=[$6], cp_description_node_9=[$7], cp_type_node_9=[$8])
         :     :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])
         :     +- LogicalProject(ca_address_sk_node_10=[$0], ca_address_id_node_10=[$1], ca_street_number_node_10=[$2], ca_street_name_node_10=[$3], ca_street_type_node_10=[$4], ca_suite_number_node_10=[$5], ca_city_node_10=[$6], ca_county_node_10=[$7], ca_state_node_10=[$8], ca_zip_node_10=[$9], ca_country_node_10=[$10], ca_gmt_offset_node_10=[$11], ca_location_type_node_10=[$12])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
         +- LogicalFilter(condition=[>=($16, -7.4744462966918945E0:DOUBLE)])
            +- LogicalSort(sort0=[$5], dir0=[ASC])
               +- LogicalProject(ss_sold_date_sk_node_11=[$0], ss_sold_time_sk_node_11=[$1], ss_item_sk_node_11=[$2], ss_customer_sk_node_11=[$3], ss_cdemo_sk_node_11=[$4], ss_hdemo_sk_node_11=[$5], ss_addr_sk_node_11=[$6], ss_store_sk_node_11=[$7], ss_promo_sk_node_11=[$8], ss_ticket_number_node_11=[$9], ss_quantity_node_11=[$10], ss_wholesale_cost_node_11=[$11], ss_list_price_node_11=[$12], ss_sales_price_node_11=[$13], ss_ext_discount_amt_node_11=[$14], ss_ext_sales_price_node_11=[$15], ss_ext_wholesale_cost_node_11=[$16], ss_ext_list_price_node_11=[$17], ss_ext_tax_node_11=[$18], ss_coupon_amt_node_11=[$19], ss_net_paid_node_11=[$20], ss_net_paid_inc_tax_node_11=[$21], ss_net_profit_node_11=[$22])
                  +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0], where=[>(CHAR_LENGTH(EXPR$0), 5)])
+- SortAggregate(isMerge=[true], groupBy=[wp_web_page_id], select=[wp_web_page_id, Final_MIN(min$0) AS EXPR$0])
   +- Sort(orderBy=[wp_web_page_id ASC])
      +- Exchange(distribution=[hash[wp_web_page_id]])
         +- LocalSortAggregate(groupBy=[wp_web_page_id], select=[wp_web_page_id, Partial_MIN(cp_type) AS min$0])
            +- Sort(orderBy=[wp_web_page_id ASC])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ca_address_sk, ss_ticket_number)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[right])
                  :- HashJoin(joinType=[InnerJoin], where=[=(ca_city, wp_rec_start_date)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], isBroadcast=[true], build=[left])
                  :  :- Exchange(distribution=[broadcast])
                  :  :  +- Limit(offset=[0], fetch=[34], global=[true])
                  :  :     +- Exchange(distribution=[single])
                  :  :        +- Limit(offset=[0], fetch=[34], global=[false])
                  :  :           +- TableSourceScan(table=[[default_catalog, default_database, web_page, limit=[34]]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                  :  +- HashJoin(joinType=[InnerJoin], where=[=(ca_address_sk, cp_catalog_number)], select=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], build=[left])
                  :     :- Exchange(distribution=[hash[cp_catalog_number]])
                  :     :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
                  :     +- Exchange(distribution=[hash[ca_address_sk]])
                  :        +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                  +- Exchange(distribution=[broadcast])
                     +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[>=(ss_ext_wholesale_cost, -7.4744462966918945E0)])
                        +- SortLimit(orderBy=[ss_hdemo_sk ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[ss_hdemo_sk ASC], offset=[0], fetch=[1], global=[false])
                                 +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0], where=[(CHAR_LENGTH(EXPR$0) > 5)])
+- SortAggregate(isMerge=[true], groupBy=[wp_web_page_id], select=[wp_web_page_id, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[wp_web_page_id ASC])
         +- Exchange(distribution=[hash[wp_web_page_id]])
            +- LocalSortAggregate(groupBy=[wp_web_page_id], select=[wp_web_page_id, Partial_MIN(cp_type) AS min$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[wp_web_page_id ASC])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(ca_address_sk = ss_ticket_number)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[right])
                        :- HashJoin(joinType=[InnerJoin], where=[(ca_city = wp_rec_start_date)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], isBroadcast=[true], build=[left])
                        :  :- Exchange(distribution=[broadcast])
                        :  :  +- Limit(offset=[0], fetch=[34], global=[true])
                        :  :     +- Exchange(distribution=[single])
                        :  :        +- Limit(offset=[0], fetch=[34], global=[false])
                        :  :           +- TableSourceScan(table=[[default_catalog, default_database, web_page, limit=[34]]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                        :  +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ca_address_sk = cp_catalog_number)], select=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], build=[left])
                        :     :- Exchange(distribution=[hash[cp_catalog_number]])
                        :     :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
                        :     +- Exchange(distribution=[hash[ca_address_sk]])
                        :        +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[(ss_ext_wholesale_cost >= -7.4744462966918945E0)])
                              +- SortLimit(orderBy=[ss_hdemo_sk ASC], offset=[0], fetch=[1], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- SortLimit(orderBy=[ss_hdemo_sk ASC], offset=[0], fetch=[1], global=[false])
                                       +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0