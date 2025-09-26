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
    return values.quantile(0.25)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_12 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_11 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_14 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_15 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_9 = autonode_13.filter(col('s_hours_node_13').char_length <= 5)
autonode_8 = autonode_11.join(autonode_12, col('ss_ext_list_price_node_12') == col('i_current_price_node_11'))
autonode_10 = autonode_14.join(autonode_15, col('sm_ship_mode_sk_node_15') == col('cs_sold_time_sk_node_14'))
autonode_6 = autonode_8.limit(92)
autonode_7 = autonode_9.join(autonode_10, col('sm_type_node_15') == col('s_country_node_13'))
autonode_4 = autonode_6.add_columns(lit("hello"))
autonode_5 = autonode_7.order_by(col('cs_sold_date_sk_node_14'))
autonode_2 = autonode_4.limit(42)
autonode_3 = autonode_5.order_by(col('cs_ship_cdemo_sk_node_14'))
autonode_1 = autonode_2.join(autonode_3, col('ss_ext_tax_node_12') == col('cs_list_price_node_14'))
sink = autonode_1.group_by(col('i_current_price_node_11')).select(col('ss_net_profit_node_12').max.alias('ss_net_profit_node_12'))
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
      "error_message": "An error occurred while calling o37347327.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#74085108:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[37](input=RelSubset#74085106,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[37]), rel#74085105:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[37](input=RelSubset#74085104,groupBy=cs_list_price,select=cs_list_price)]
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
LogicalProject(ss_net_profit_node_12=[$1])
+- LogicalAggregate(group=[{5}], EXPR$0=[MAX($44)])
   +- LogicalJoin(condition=[=($40, $95)], joinType=[inner])
      :- LogicalSort(fetch=[42])
      :  +- LogicalProject(i_item_sk_node_11=[$0], i_item_id_node_11=[$1], i_rec_start_date_node_11=[$2], i_rec_end_date_node_11=[$3], i_item_desc_node_11=[$4], i_current_price_node_11=[$5], i_wholesale_cost_node_11=[$6], i_brand_id_node_11=[$7], i_brand_node_11=[$8], i_class_id_node_11=[$9], i_class_node_11=[$10], i_category_id_node_11=[$11], i_category_node_11=[$12], i_manufact_id_node_11=[$13], i_manufact_node_11=[$14], i_size_node_11=[$15], i_formulation_node_11=[$16], i_color_node_11=[$17], i_units_node_11=[$18], i_container_node_11=[$19], i_manager_id_node_11=[$20], i_product_name_node_11=[$21], ss_sold_date_sk_node_12=[$22], ss_sold_time_sk_node_12=[$23], ss_item_sk_node_12=[$24], ss_customer_sk_node_12=[$25], ss_cdemo_sk_node_12=[$26], ss_hdemo_sk_node_12=[$27], ss_addr_sk_node_12=[$28], ss_store_sk_node_12=[$29], ss_promo_sk_node_12=[$30], ss_ticket_number_node_12=[$31], ss_quantity_node_12=[$32], ss_wholesale_cost_node_12=[$33], ss_list_price_node_12=[$34], ss_sales_price_node_12=[$35], ss_ext_discount_amt_node_12=[$36], ss_ext_sales_price_node_12=[$37], ss_ext_wholesale_cost_node_12=[$38], ss_ext_list_price_node_12=[$39], ss_ext_tax_node_12=[$40], ss_coupon_amt_node_12=[$41], ss_net_paid_node_12=[$42], ss_net_paid_inc_tax_node_12=[$43], ss_net_profit_node_12=[$44], _c45=[_UTF-16LE'hello'])
      :     +- LogicalSort(fetch=[92])
      :        +- LogicalJoin(condition=[=($39, $5)], joinType=[inner])
      :           :- LogicalProject(i_item_sk_node_11=[$0], i_item_id_node_11=[$1], i_rec_start_date_node_11=[$2], i_rec_end_date_node_11=[$3], i_item_desc_node_11=[$4], i_current_price_node_11=[$5], i_wholesale_cost_node_11=[$6], i_brand_id_node_11=[$7], i_brand_node_11=[$8], i_class_id_node_11=[$9], i_class_node_11=[$10], i_category_id_node_11=[$11], i_category_node_11=[$12], i_manufact_id_node_11=[$13], i_manufact_node_11=[$14], i_size_node_11=[$15], i_formulation_node_11=[$16], i_color_node_11=[$17], i_units_node_11=[$18], i_container_node_11=[$19], i_manager_id_node_11=[$20], i_product_name_node_11=[$21])
      :           :  +- LogicalTableScan(table=[[default_catalog, default_database, item]])
      :           +- LogicalProject(ss_sold_date_sk_node_12=[$0], ss_sold_time_sk_node_12=[$1], ss_item_sk_node_12=[$2], ss_customer_sk_node_12=[$3], ss_cdemo_sk_node_12=[$4], ss_hdemo_sk_node_12=[$5], ss_addr_sk_node_12=[$6], ss_store_sk_node_12=[$7], ss_promo_sk_node_12=[$8], ss_ticket_number_node_12=[$9], ss_quantity_node_12=[$10], ss_wholesale_cost_node_12=[$11], ss_list_price_node_12=[$12], ss_sales_price_node_12=[$13], ss_ext_discount_amt_node_12=[$14], ss_ext_sales_price_node_12=[$15], ss_ext_wholesale_cost_node_12=[$16], ss_ext_list_price_node_12=[$17], ss_ext_tax_node_12=[$18], ss_coupon_amt_node_12=[$19], ss_net_paid_node_12=[$20], ss_net_paid_inc_tax_node_12=[$21], ss_net_profit_node_12=[$22])
      :              +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalSort(sort0=[$37], dir0=[ASC])
         +- LogicalSort(sort0=[$29], dir0=[ASC])
            +- LogicalJoin(condition=[=($65, $26)], joinType=[inner])
               :- LogicalFilter(condition=[<=(CHAR_LENGTH($8), 5)])
               :  +- LogicalProject(s_store_sk_node_13=[$0], s_store_id_node_13=[$1], s_rec_start_date_node_13=[$2], s_rec_end_date_node_13=[$3], s_closed_date_sk_node_13=[$4], s_store_name_node_13=[$5], s_number_employees_node_13=[$6], s_floor_space_node_13=[$7], s_hours_node_13=[$8], s_manager_node_13=[$9], s_market_id_node_13=[$10], s_geography_class_node_13=[$11], s_market_desc_node_13=[$12], s_market_manager_node_13=[$13], s_division_id_node_13=[$14], s_division_name_node_13=[$15], s_company_id_node_13=[$16], s_company_name_node_13=[$17], s_street_number_node_13=[$18], s_street_name_node_13=[$19], s_street_type_node_13=[$20], s_suite_number_node_13=[$21], s_city_node_13=[$22], s_county_node_13=[$23], s_state_node_13=[$24], s_zip_node_13=[$25], s_country_node_13=[$26], s_gmt_offset_node_13=[$27], s_tax_precentage_node_13=[$28])
               :     +- LogicalTableScan(table=[[default_catalog, default_database, store]])
               +- LogicalJoin(condition=[=($34, $1)], joinType=[inner])
                  :- LogicalProject(cs_sold_date_sk_node_14=[$0], cs_sold_time_sk_node_14=[$1], cs_ship_date_sk_node_14=[$2], cs_bill_customer_sk_node_14=[$3], cs_bill_cdemo_sk_node_14=[$4], cs_bill_hdemo_sk_node_14=[$5], cs_bill_addr_sk_node_14=[$6], cs_ship_customer_sk_node_14=[$7], cs_ship_cdemo_sk_node_14=[$8], cs_ship_hdemo_sk_node_14=[$9], cs_ship_addr_sk_node_14=[$10], cs_call_center_sk_node_14=[$11], cs_catalog_page_sk_node_14=[$12], cs_ship_mode_sk_node_14=[$13], cs_warehouse_sk_node_14=[$14], cs_item_sk_node_14=[$15], cs_promo_sk_node_14=[$16], cs_order_number_node_14=[$17], cs_quantity_node_14=[$18], cs_wholesale_cost_node_14=[$19], cs_list_price_node_14=[$20], cs_sales_price_node_14=[$21], cs_ext_discount_amt_node_14=[$22], cs_ext_sales_price_node_14=[$23], cs_ext_wholesale_cost_node_14=[$24], cs_ext_list_price_node_14=[$25], cs_ext_tax_node_14=[$26], cs_coupon_amt_node_14=[$27], cs_ext_ship_cost_node_14=[$28], cs_net_paid_node_14=[$29], cs_net_paid_inc_tax_node_14=[$30], cs_net_paid_inc_ship_node_14=[$31], cs_net_paid_inc_ship_tax_node_14=[$32], cs_net_profit_node_14=[$33])
                  :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
                  +- LogicalProject(sm_ship_mode_sk_node_15=[$0], sm_ship_mode_id_node_15=[$1], sm_type_node_15=[$2], sm_code_node_15=[$3], sm_carrier_node_15=[$4], sm_contract_node_15=[$5])
                     +- LogicalTableScan(table=[[default_catalog, default_database, ship_mode]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ss_net_profit_node_12])
+- SortAggregate(isMerge=[true], groupBy=[i_current_price_node_11], select=[i_current_price_node_11, Final_MAX(max$0) AS EXPR$0])
   +- Sort(orderBy=[i_current_price_node_11 ASC])
      +- Exchange(distribution=[hash[i_current_price_node_11]])
         +- LocalSortAggregate(groupBy=[i_current_price_node_11], select=[i_current_price_node_11, Partial_MAX(ss_net_profit_node_12) AS max$0])
            +- Sort(orderBy=[i_current_price_node_11 ASC])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_ext_tax_node_12, cs_list_price)], select=[i_item_sk_node_11, i_item_id_node_11, i_rec_start_date_node_11, i_rec_end_date_node_11, i_item_desc_node_11, i_current_price_node_11, i_wholesale_cost_node_11, i_brand_id_node_11, i_brand_node_11, i_class_id_node_11, i_class_node_11, i_category_id_node_11, i_category_node_11, i_manufact_id_node_11, i_manufact_node_11, i_size_node_11, i_formulation_node_11, i_color_node_11, i_units_node_11, i_container_node_11, i_manager_id_node_11, i_product_name_node_11, ss_sold_date_sk_node_12, ss_sold_time_sk_node_12, ss_item_sk_node_12, ss_customer_sk_node_12, ss_cdemo_sk_node_12, ss_hdemo_sk_node_12, ss_addr_sk_node_12, ss_store_sk_node_12, ss_promo_sk_node_12, ss_ticket_number_node_12, ss_quantity_node_12, ss_wholesale_cost_node_12, ss_list_price_node_12, ss_sales_price_node_12, ss_ext_discount_amt_node_12, ss_ext_sales_price_node_12, ss_ext_wholesale_cost_node_12, ss_ext_list_price_node_12, ss_ext_tax_node_12, ss_coupon_amt_node_12, ss_net_paid_node_12, ss_net_paid_inc_tax_node_12, ss_net_profit_node_12, _c45, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], build=[right])
                  :- Calc(select=[i_item_sk AS i_item_sk_node_11, i_item_id AS i_item_id_node_11, i_rec_start_date AS i_rec_start_date_node_11, i_rec_end_date AS i_rec_end_date_node_11, i_item_desc AS i_item_desc_node_11, i_current_price AS i_current_price_node_11, i_wholesale_cost AS i_wholesale_cost_node_11, i_brand_id AS i_brand_id_node_11, i_brand AS i_brand_node_11, i_class_id AS i_class_id_node_11, i_class AS i_class_node_11, i_category_id AS i_category_id_node_11, i_category AS i_category_node_11, i_manufact_id AS i_manufact_id_node_11, i_manufact AS i_manufact_node_11, i_size AS i_size_node_11, i_formulation AS i_formulation_node_11, i_color AS i_color_node_11, i_units AS i_units_node_11, i_container AS i_container_node_11, i_manager_id AS i_manager_id_node_11, i_product_name AS i_product_name_node_11, ss_sold_date_sk AS ss_sold_date_sk_node_12, ss_sold_time_sk AS ss_sold_time_sk_node_12, ss_item_sk AS ss_item_sk_node_12, ss_customer_sk AS ss_customer_sk_node_12, ss_cdemo_sk AS ss_cdemo_sk_node_12, ss_hdemo_sk AS ss_hdemo_sk_node_12, ss_addr_sk AS ss_addr_sk_node_12, ss_store_sk AS ss_store_sk_node_12, ss_promo_sk AS ss_promo_sk_node_12, ss_ticket_number AS ss_ticket_number_node_12, ss_quantity AS ss_quantity_node_12, ss_wholesale_cost AS ss_wholesale_cost_node_12, ss_list_price AS ss_list_price_node_12, ss_sales_price AS ss_sales_price_node_12, ss_ext_discount_amt AS ss_ext_discount_amt_node_12, ss_ext_sales_price AS ss_ext_sales_price_node_12, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_12, ss_ext_list_price AS ss_ext_list_price_node_12, ss_ext_tax AS ss_ext_tax_node_12, ss_coupon_amt AS ss_coupon_amt_node_12, ss_net_paid AS ss_net_paid_node_12, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_12, ss_net_profit AS ss_net_profit_node_12, 'hello' AS _c45])
                  :  +- Limit(offset=[0], fetch=[42], global=[true])
                  :     +- Exchange(distribution=[single])
                  :        +- Limit(offset=[0], fetch=[42], global=[false])
                  :           +- Limit(offset=[0], fetch=[92], global=[true])
                  :              +- Exchange(distribution=[single])
                  :                 +- Limit(offset=[0], fetch=[92], global=[false])
                  :                    +- HashJoin(joinType=[InnerJoin], where=[=(ss_ext_list_price, i_current_price)], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
                  :                       :- Exchange(distribution=[hash[i_current_price]])
                  :                       :  +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
                  :                       +- Exchange(distribution=[hash[ss_ext_list_price]])
                  :                          +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                  +- Exchange(distribution=[broadcast])
                     +- SortLimit(orderBy=[cs_ship_cdemo_sk ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[cs_ship_cdemo_sk ASC], offset=[0], fetch=[1], global=[false])
                              +- SortLimit(orderBy=[cs_sold_date_sk ASC], offset=[0], fetch=[1], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- SortLimit(orderBy=[cs_sold_date_sk ASC], offset=[0], fetch=[1], global=[false])
                                       +- NestedLoopJoin(joinType=[InnerJoin], where=[=(sm_type, s_country)], select=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], build=[left])
                                          :- Exchange(distribution=[broadcast])
                                          :  +- Calc(select=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], where=[<=(CHAR_LENGTH(s_hours), 5)])
                                          :     +- TableSourceScan(table=[[default_catalog, default_database, store, filter=[<=(CHAR_LENGTH(s_hours), 5)]]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                                          +- HashJoin(joinType=[InnerJoin], where=[=(sm_ship_mode_sk, cs_sold_time_sk)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], isBroadcast=[true], build=[right])
                                             :- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
                                             +- Exchange(distribution=[broadcast])
                                                +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ss_net_profit_node_12])
+- SortAggregate(isMerge=[true], groupBy=[i_current_price_node_11], select=[i_current_price_node_11, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[i_current_price_node_11 ASC])
         +- Exchange(distribution=[hash[i_current_price_node_11]])
            +- LocalSortAggregate(groupBy=[i_current_price_node_11], select=[i_current_price_node_11, Partial_MAX(ss_net_profit_node_12) AS max$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[i_current_price_node_11 ASC])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_ext_tax_node_12 = cs_list_price)], select=[i_item_sk_node_11, i_item_id_node_11, i_rec_start_date_node_11, i_rec_end_date_node_11, i_item_desc_node_11, i_current_price_node_11, i_wholesale_cost_node_11, i_brand_id_node_11, i_brand_node_11, i_class_id_node_11, i_class_node_11, i_category_id_node_11, i_category_node_11, i_manufact_id_node_11, i_manufact_node_11, i_size_node_11, i_formulation_node_11, i_color_node_11, i_units_node_11, i_container_node_11, i_manager_id_node_11, i_product_name_node_11, ss_sold_date_sk_node_12, ss_sold_time_sk_node_12, ss_item_sk_node_12, ss_customer_sk_node_12, ss_cdemo_sk_node_12, ss_hdemo_sk_node_12, ss_addr_sk_node_12, ss_store_sk_node_12, ss_promo_sk_node_12, ss_ticket_number_node_12, ss_quantity_node_12, ss_wholesale_cost_node_12, ss_list_price_node_12, ss_sales_price_node_12, ss_ext_discount_amt_node_12, ss_ext_sales_price_node_12, ss_ext_wholesale_cost_node_12, ss_ext_list_price_node_12, ss_ext_tax_node_12, ss_coupon_amt_node_12, ss_net_paid_node_12, ss_net_paid_inc_tax_node_12, ss_net_profit_node_12, _c45, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], build=[right])
                        :- Calc(select=[i_item_sk AS i_item_sk_node_11, i_item_id AS i_item_id_node_11, i_rec_start_date AS i_rec_start_date_node_11, i_rec_end_date AS i_rec_end_date_node_11, i_item_desc AS i_item_desc_node_11, i_current_price AS i_current_price_node_11, i_wholesale_cost AS i_wholesale_cost_node_11, i_brand_id AS i_brand_id_node_11, i_brand AS i_brand_node_11, i_class_id AS i_class_id_node_11, i_class AS i_class_node_11, i_category_id AS i_category_id_node_11, i_category AS i_category_node_11, i_manufact_id AS i_manufact_id_node_11, i_manufact AS i_manufact_node_11, i_size AS i_size_node_11, i_formulation AS i_formulation_node_11, i_color AS i_color_node_11, i_units AS i_units_node_11, i_container AS i_container_node_11, i_manager_id AS i_manager_id_node_11, i_product_name AS i_product_name_node_11, ss_sold_date_sk AS ss_sold_date_sk_node_12, ss_sold_time_sk AS ss_sold_time_sk_node_12, ss_item_sk AS ss_item_sk_node_12, ss_customer_sk AS ss_customer_sk_node_12, ss_cdemo_sk AS ss_cdemo_sk_node_12, ss_hdemo_sk AS ss_hdemo_sk_node_12, ss_addr_sk AS ss_addr_sk_node_12, ss_store_sk AS ss_store_sk_node_12, ss_promo_sk AS ss_promo_sk_node_12, ss_ticket_number AS ss_ticket_number_node_12, ss_quantity AS ss_quantity_node_12, ss_wholesale_cost AS ss_wholesale_cost_node_12, ss_list_price AS ss_list_price_node_12, ss_sales_price AS ss_sales_price_node_12, ss_ext_discount_amt AS ss_ext_discount_amt_node_12, ss_ext_sales_price AS ss_ext_sales_price_node_12, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_12, ss_ext_list_price AS ss_ext_list_price_node_12, ss_ext_tax AS ss_ext_tax_node_12, ss_coupon_amt AS ss_coupon_amt_node_12, ss_net_paid AS ss_net_paid_node_12, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_12, ss_net_profit AS ss_net_profit_node_12, 'hello' AS _c45])
                        :  +- Limit(offset=[0], fetch=[42], global=[true])
                        :     +- Exchange(distribution=[single])
                        :        +- Limit(offset=[0], fetch=[42], global=[false])
                        :           +- Limit(offset=[0], fetch=[92], global=[true])
                        :              +- Exchange(distribution=[single])
                        :                 +- Limit(offset=[0], fetch=[92], global=[false])
                        :                    +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ss_ext_list_price = i_current_price)], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
                        :                       :- Exchange(distribution=[hash[i_current_price]])
                        :                       :  +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
                        :                       +- Exchange(distribution=[hash[ss_ext_list_price]])
                        :                          +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                        +- Exchange(distribution=[broadcast])
                           +- SortLimit(orderBy=[cs_ship_cdemo_sk ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[cs_ship_cdemo_sk ASC], offset=[0], fetch=[1], global=[false])
                                    +- SortLimit(orderBy=[cs_sold_date_sk ASC], offset=[0], fetch=[1], global=[true])
                                       +- Exchange(distribution=[single])
                                          +- SortLimit(orderBy=[cs_sold_date_sk ASC], offset=[0], fetch=[1], global=[false])
                                             +- NestedLoopJoin(joinType=[InnerJoin], where=[(sm_type = s_country)], select=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], build=[left])
                                                :- Exchange(distribution=[broadcast])
                                                :  +- Calc(select=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], where=[(CHAR_LENGTH(s_hours) <= 5)])
                                                :     +- TableSourceScan(table=[[default_catalog, default_database, store, filter=[<=(CHAR_LENGTH(s_hours), 5)]]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                                                +- HashJoin(joinType=[InnerJoin], where=[(sm_ship_mode_sk = cs_sold_time_sk)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], isBroadcast=[true], build=[right])
                                                   :- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
                                                   +- Exchange(distribution=[broadcast])
                                                      +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0