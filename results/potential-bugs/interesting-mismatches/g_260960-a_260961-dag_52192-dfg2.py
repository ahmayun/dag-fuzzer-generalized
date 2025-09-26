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
    return values.var()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_12 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_11 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_14 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_15 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_16 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_8 = autonode_12.join(autonode_13, col('cc_tax_percentage_node_12') == col('ws_ext_ship_cost_node_13'))
autonode_7 = autonode_11.distinct()
autonode_9 = autonode_14.join(autonode_15, col('inv_warehouse_sk_node_14') == col('s_closed_date_sk_node_15'))
autonode_10 = autonode_16.order_by(col('web_city_node_16'))
autonode_5 = autonode_8.filter(col('ws_net_paid_inc_ship_node_13') < 34.403008222579956)
autonode_4 = autonode_7.order_by(col('cd_dep_college_count_node_11'))
autonode_6 = autonode_9.join(autonode_10, col('s_hours_node_15') == col('web_suite_number_node_16'))
autonode_2 = autonode_4.distinct()
autonode_3 = autonode_5.join(autonode_6, col('ws_quantity_node_13') == col('inv_item_sk_node_14'))
autonode_1 = autonode_2.join(autonode_3, col('cd_purchase_estimate_node_11') == col('inv_date_sk_node_14'))
sink = autonode_1.group_by(col('ws_item_sk_node_13')).select(col('web_street_type_node_16').min.alias('web_street_type_node_16'))
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
      "error_message": "An error occurred while calling o142050304.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#286947068:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[8](input=RelSubset#286947066,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[8]), rel#286947065:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[8](input=RelSubset#286947064,groupBy=cd_purchase_estimate,select=cd_purchase_estimate)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (8) must be less than size (1)
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
LogicalProject(web_street_type_node_16=[$1])
+- LogicalAggregate(group=[{43}], EXPR$0=[MIN($124)])
   +- LogicalJoin(condition=[=($4, $74)], joinType=[inner])
      :- LogicalSort(sort0=[$8], dir0=[ASC])
      :  +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8}])
      :     +- LogicalProject(cd_demo_sk_node_11=[$0], cd_gender_node_11=[$1], cd_marital_status_node_11=[$2], cd_education_status_node_11=[$3], cd_purchase_estimate_node_11=[$4], cd_credit_rating_node_11=[$5], cd_dep_count_node_11=[$6], cd_dep_employed_count_node_11=[$7], cd_dep_college_count_node_11=[$8])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
      +- LogicalJoin(condition=[=($49, $66)], joinType=[inner])
         :- LogicalFilter(condition=[<($62, 3.4403008222579956E1:DOUBLE)])
         :  +- LogicalJoin(condition=[=($30, $59)], joinType=[inner])
         :     :- LogicalProject(cc_call_center_sk_node_12=[$0], cc_call_center_id_node_12=[$1], cc_rec_start_date_node_12=[$2], cc_rec_end_date_node_12=[$3], cc_closed_date_sk_node_12=[$4], cc_open_date_sk_node_12=[$5], cc_name_node_12=[$6], cc_class_node_12=[$7], cc_employees_node_12=[$8], cc_sq_ft_node_12=[$9], cc_hours_node_12=[$10], cc_manager_node_12=[$11], cc_mkt_id_node_12=[$12], cc_mkt_class_node_12=[$13], cc_mkt_desc_node_12=[$14], cc_market_manager_node_12=[$15], cc_division_node_12=[$16], cc_division_name_node_12=[$17], cc_company_node_12=[$18], cc_company_name_node_12=[$19], cc_street_number_node_12=[$20], cc_street_name_node_12=[$21], cc_street_type_node_12=[$22], cc_suite_number_node_12=[$23], cc_city_node_12=[$24], cc_county_node_12=[$25], cc_state_node_12=[$26], cc_zip_node_12=[$27], cc_country_node_12=[$28], cc_gmt_offset_node_12=[$29], cc_tax_percentage_node_12=[$30])
         :     :  +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])
         :     +- LogicalProject(ws_sold_date_sk_node_13=[$0], ws_sold_time_sk_node_13=[$1], ws_ship_date_sk_node_13=[$2], ws_item_sk_node_13=[$3], ws_bill_customer_sk_node_13=[$4], ws_bill_cdemo_sk_node_13=[$5], ws_bill_hdemo_sk_node_13=[$6], ws_bill_addr_sk_node_13=[$7], ws_ship_customer_sk_node_13=[$8], ws_ship_cdemo_sk_node_13=[$9], ws_ship_hdemo_sk_node_13=[$10], ws_ship_addr_sk_node_13=[$11], ws_web_page_sk_node_13=[$12], ws_web_site_sk_node_13=[$13], ws_ship_mode_sk_node_13=[$14], ws_warehouse_sk_node_13=[$15], ws_promo_sk_node_13=[$16], ws_order_number_node_13=[$17], ws_quantity_node_13=[$18], ws_wholesale_cost_node_13=[$19], ws_list_price_node_13=[$20], ws_sales_price_node_13=[$21], ws_ext_discount_amt_node_13=[$22], ws_ext_sales_price_node_13=[$23], ws_ext_wholesale_cost_node_13=[$24], ws_ext_list_price_node_13=[$25], ws_ext_tax_node_13=[$26], ws_coupon_amt_node_13=[$27], ws_ext_ship_cost_node_13=[$28], ws_net_paid_node_13=[$29], ws_net_paid_inc_tax_node_13=[$30], ws_net_paid_inc_ship_node_13=[$31], ws_net_paid_inc_ship_tax_node_13=[$32], ws_net_profit_node_13=[$33])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
         +- LogicalJoin(condition=[=($12, $51)], joinType=[inner])
            :- LogicalJoin(condition=[=($2, $8)], joinType=[inner])
            :  :- LogicalProject(inv_date_sk_node_14=[$0], inv_item_sk_node_14=[$1], inv_warehouse_sk_node_14=[$2], inv_quantity_on_hand_node_14=[$3])
            :  :  +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])
            :  +- LogicalProject(s_store_sk_node_15=[$0], s_store_id_node_15=[$1], s_rec_start_date_node_15=[$2], s_rec_end_date_node_15=[$3], s_closed_date_sk_node_15=[$4], s_store_name_node_15=[$5], s_number_employees_node_15=[$6], s_floor_space_node_15=[$7], s_hours_node_15=[$8], s_manager_node_15=[$9], s_market_id_node_15=[$10], s_geography_class_node_15=[$11], s_market_desc_node_15=[$12], s_market_manager_node_15=[$13], s_division_id_node_15=[$14], s_division_name_node_15=[$15], s_company_id_node_15=[$16], s_company_name_node_15=[$17], s_street_number_node_15=[$18], s_street_name_node_15=[$19], s_street_type_node_15=[$20], s_suite_number_node_15=[$21], s_city_node_15=[$22], s_county_node_15=[$23], s_state_node_15=[$24], s_zip_node_15=[$25], s_country_node_15=[$26], s_gmt_offset_node_15=[$27], s_tax_precentage_node_15=[$28])
            :     +- LogicalTableScan(table=[[default_catalog, default_database, store]])
            +- LogicalSort(sort0=[$19], dir0=[ASC])
               +- LogicalProject(web_site_sk_node_16=[$0], web_site_id_node_16=[$1], web_rec_start_date_node_16=[$2], web_rec_end_date_node_16=[$3], web_name_node_16=[$4], web_open_date_sk_node_16=[$5], web_close_date_sk_node_16=[$6], web_class_node_16=[$7], web_manager_node_16=[$8], web_mkt_id_node_16=[$9], web_mkt_class_node_16=[$10], web_mkt_desc_node_16=[$11], web_market_manager_node_16=[$12], web_company_id_node_16=[$13], web_company_name_node_16=[$14], web_street_number_node_16=[$15], web_street_name_node_16=[$16], web_street_type_node_16=[$17], web_suite_number_node_16=[$18], web_city_node_16=[$19], web_county_node_16=[$20], web_state_node_16=[$21], web_zip_node_16=[$22], web_country_node_16=[$23], web_gmt_offset_node_16=[$24], web_tax_percentage_node_16=[$25])
                  +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS web_street_type_node_16])
+- SortAggregate(isMerge=[false], groupBy=[ws_item_sk_node_13], select=[ws_item_sk_node_13, MIN(web_street_type) AS EXPR$0])
   +- Sort(orderBy=[ws_item_sk_node_13 ASC])
      +- Exchange(distribution=[hash[ws_item_sk_node_13]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cd_purchase_estimate, inv_date_sk)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, cc_call_center_sk_node_12, cc_call_center_id_node_12, cc_rec_start_date_node_12, cc_rec_end_date_node_12, cc_closed_date_sk_node_12, cc_open_date_sk_node_12, cc_name_node_12, cc_class_node_12, cc_employees_node_12, cc_sq_ft_node_12, cc_hours_node_12, cc_manager_node_12, cc_mkt_id_node_12, cc_mkt_class_node_12, cc_mkt_desc_node_12, cc_market_manager_node_12, cc_division_node_12, cc_division_name_node_12, cc_company_node_12, cc_company_name_node_12, cc_street_number_node_12, cc_street_name_node_12, cc_street_type_node_12, cc_suite_number_node_12, cc_city_node_12, cc_county_node_12, cc_state_node_12, cc_zip_node_12, cc_country_node_12, cc_gmt_offset_node_12, cc_tax_percentage_node_12, ws_sold_date_sk_node_13, ws_sold_time_sk_node_13, ws_ship_date_sk_node_13, ws_item_sk_node_13, ws_bill_customer_sk_node_13, ws_bill_cdemo_sk_node_13, ws_bill_hdemo_sk_node_13, ws_bill_addr_sk_node_13, ws_ship_customer_sk_node_13, ws_ship_cdemo_sk_node_13, ws_ship_hdemo_sk_node_13, ws_ship_addr_sk_node_13, ws_web_page_sk_node_13, ws_web_site_sk_node_13, ws_ship_mode_sk_node_13, ws_warehouse_sk_node_13, ws_promo_sk_node_13, ws_order_number_node_13, ws_quantity_node_13, ws_wholesale_cost_node_13, ws_list_price_node_13, ws_sales_price_node_13, ws_ext_discount_amt_node_13, ws_ext_sales_price_node_13, ws_ext_wholesale_cost_node_13, ws_ext_list_price_node_13, ws_ext_tax_node_13, ws_coupon_amt_node_13, ws_ext_ship_cost_node_13, ws_net_paid_node_13, ws_net_paid_inc_tax_node_13, ws_net_paid_inc_ship_node_13, ws_net_paid_inc_ship_tax_node_13, ws_net_profit_node_13, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- SortLimit(orderBy=[cd_dep_college_count ASC], offset=[0], fetch=[1], global=[true])
            :     +- Exchange(distribution=[single])
            :        +- SortLimit(orderBy=[cd_dep_college_count ASC], offset=[0], fetch=[1], global=[false])
            :           +- HashAggregate(isMerge=[true], groupBy=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
            :              +- Exchange(distribution=[hash[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count]])
            :                 +- LocalHashAggregate(groupBy=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
            :                    +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
            +- HashJoin(joinType=[InnerJoin], where=[=(ws_quantity_node_13, inv_item_sk)], select=[cc_call_center_sk_node_12, cc_call_center_id_node_12, cc_rec_start_date_node_12, cc_rec_end_date_node_12, cc_closed_date_sk_node_12, cc_open_date_sk_node_12, cc_name_node_12, cc_class_node_12, cc_employees_node_12, cc_sq_ft_node_12, cc_hours_node_12, cc_manager_node_12, cc_mkt_id_node_12, cc_mkt_class_node_12, cc_mkt_desc_node_12, cc_market_manager_node_12, cc_division_node_12, cc_division_name_node_12, cc_company_node_12, cc_company_name_node_12, cc_street_number_node_12, cc_street_name_node_12, cc_street_type_node_12, cc_suite_number_node_12, cc_city_node_12, cc_county_node_12, cc_state_node_12, cc_zip_node_12, cc_country_node_12, cc_gmt_offset_node_12, cc_tax_percentage_node_12, ws_sold_date_sk_node_13, ws_sold_time_sk_node_13, ws_ship_date_sk_node_13, ws_item_sk_node_13, ws_bill_customer_sk_node_13, ws_bill_cdemo_sk_node_13, ws_bill_hdemo_sk_node_13, ws_bill_addr_sk_node_13, ws_ship_customer_sk_node_13, ws_ship_cdemo_sk_node_13, ws_ship_hdemo_sk_node_13, ws_ship_addr_sk_node_13, ws_web_page_sk_node_13, ws_web_site_sk_node_13, ws_ship_mode_sk_node_13, ws_warehouse_sk_node_13, ws_promo_sk_node_13, ws_order_number_node_13, ws_quantity_node_13, ws_wholesale_cost_node_13, ws_list_price_node_13, ws_sales_price_node_13, ws_ext_discount_amt_node_13, ws_ext_sales_price_node_13, ws_ext_wholesale_cost_node_13, ws_ext_list_price_node_13, ws_ext_tax_node_13, ws_coupon_amt_node_13, ws_ext_ship_cost_node_13, ws_net_paid_node_13, ws_net_paid_inc_tax_node_13, ws_net_paid_inc_ship_node_13, ws_net_paid_inc_ship_tax_node_13, ws_net_profit_node_13, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], build=[left])
               :- Exchange(distribution=[hash[ws_quantity_node_13]])
               :  +- Calc(select=[cc_call_center_sk_node_12, cc_call_center_id_node_12, cc_rec_start_date_node_12, cc_rec_end_date_node_12, cc_closed_date_sk_node_12, cc_open_date_sk_node_12, cc_name_node_12, cc_class_node_12, cc_employees_node_12, cc_sq_ft_node_12, cc_hours_node_12, cc_manager_node_12, cc_mkt_id_node_12, cc_mkt_class_node_12, cc_mkt_desc_node_12, cc_market_manager_node_12, cc_division_node_12, cc_division_name_node_12, cc_company_node_12, cc_company_name_node_12, cc_street_number_node_12, cc_street_name_node_12, cc_street_type_node_12, cc_suite_number_node_12, cc_city_node_12, cc_county_node_12, cc_state_node_12, cc_zip_node_12, cc_country_node_12, cc_gmt_offset_node_12, cc_tax_percentage_node_12, ws_sold_date_sk AS ws_sold_date_sk_node_13, ws_sold_time_sk AS ws_sold_time_sk_node_13, ws_ship_date_sk AS ws_ship_date_sk_node_13, ws_item_sk AS ws_item_sk_node_13, ws_bill_customer_sk AS ws_bill_customer_sk_node_13, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_13, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_13, ws_bill_addr_sk AS ws_bill_addr_sk_node_13, ws_ship_customer_sk AS ws_ship_customer_sk_node_13, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_13, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_13, ws_ship_addr_sk AS ws_ship_addr_sk_node_13, ws_web_page_sk AS ws_web_page_sk_node_13, ws_web_site_sk AS ws_web_site_sk_node_13, ws_ship_mode_sk AS ws_ship_mode_sk_node_13, ws_warehouse_sk AS ws_warehouse_sk_node_13, ws_promo_sk AS ws_promo_sk_node_13, ws_order_number AS ws_order_number_node_13, ws_quantity AS ws_quantity_node_13, ws_wholesale_cost AS ws_wholesale_cost_node_13, ws_list_price AS ws_list_price_node_13, ws_sales_price AS ws_sales_price_node_13, ws_ext_discount_amt AS ws_ext_discount_amt_node_13, ws_ext_sales_price AS ws_ext_sales_price_node_13, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_13, ws_ext_list_price AS ws_ext_list_price_node_13, ws_ext_tax AS ws_ext_tax_node_13, ws_coupon_amt AS ws_coupon_amt_node_13, ws_ext_ship_cost AS ws_ext_ship_cost_node_13, ws_net_paid AS ws_net_paid_node_13, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_13, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_13, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_13, ws_net_profit AS ws_net_profit_node_13])
               :     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cc_tax_percentage_node_120, ws_ext_ship_cost)], select=[cc_call_center_sk_node_12, cc_call_center_id_node_12, cc_rec_start_date_node_12, cc_rec_end_date_node_12, cc_closed_date_sk_node_12, cc_open_date_sk_node_12, cc_name_node_12, cc_class_node_12, cc_employees_node_12, cc_sq_ft_node_12, cc_hours_node_12, cc_manager_node_12, cc_mkt_id_node_12, cc_mkt_class_node_12, cc_mkt_desc_node_12, cc_market_manager_node_12, cc_division_node_12, cc_division_name_node_12, cc_company_node_12, cc_company_name_node_12, cc_street_number_node_12, cc_street_name_node_12, cc_street_type_node_12, cc_suite_number_node_12, cc_city_node_12, cc_county_node_12, cc_state_node_12, cc_zip_node_12, cc_country_node_12, cc_gmt_offset_node_12, cc_tax_percentage_node_12, cc_tax_percentage_node_120, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
               :        :- Exchange(distribution=[broadcast])
               :        :  +- Calc(select=[cc_call_center_sk AS cc_call_center_sk_node_12, cc_call_center_id AS cc_call_center_id_node_12, cc_rec_start_date AS cc_rec_start_date_node_12, cc_rec_end_date AS cc_rec_end_date_node_12, cc_closed_date_sk AS cc_closed_date_sk_node_12, cc_open_date_sk AS cc_open_date_sk_node_12, cc_name AS cc_name_node_12, cc_class AS cc_class_node_12, cc_employees AS cc_employees_node_12, cc_sq_ft AS cc_sq_ft_node_12, cc_hours AS cc_hours_node_12, cc_manager AS cc_manager_node_12, cc_mkt_id AS cc_mkt_id_node_12, cc_mkt_class AS cc_mkt_class_node_12, cc_mkt_desc AS cc_mkt_desc_node_12, cc_market_manager AS cc_market_manager_node_12, cc_division AS cc_division_node_12, cc_division_name AS cc_division_name_node_12, cc_company AS cc_company_node_12, cc_company_name AS cc_company_name_node_12, cc_street_number AS cc_street_number_node_12, cc_street_name AS cc_street_name_node_12, cc_street_type AS cc_street_type_node_12, cc_suite_number AS cc_suite_number_node_12, cc_city AS cc_city_node_12, cc_county AS cc_county_node_12, cc_state AS cc_state_node_12, cc_zip AS cc_zip_node_12, cc_country AS cc_country_node_12, cc_gmt_offset AS cc_gmt_offset_node_12, cc_tax_percentage AS cc_tax_percentage_node_12, CAST(cc_tax_percentage AS DECIMAL(7, 2)) AS cc_tax_percentage_node_120])
               :        :     +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
               :        +- Calc(select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], where=[<(ws_net_paid_inc_ship, 3.4403008222579956E1)])
               :           +- TableSourceScan(table=[[default_catalog, default_database, web_sales, filter=[<(ws_net_paid_inc_ship, 3.4403008222579956E1:DOUBLE)]]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
               +- Exchange(distribution=[hash[inv_item_sk]])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(s_hours, web_suite_number)], select=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], build=[right])
                     :- HashJoin(joinType=[InnerJoin], where=[=(inv_warehouse_sk, s_closed_date_sk)], select=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], isBroadcast=[true], build=[right])
                     :  :- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
                     :  +- Exchange(distribution=[broadcast])
                     :     +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                     +- Exchange(distribution=[broadcast])
                        +- SortLimit(orderBy=[web_city ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[web_city ASC], offset=[0], fetch=[1], global=[false])
                                 +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS web_street_type_node_16])
+- SortAggregate(isMerge=[false], groupBy=[ws_item_sk_node_13], select=[ws_item_sk_node_13, MIN(web_street_type) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[ws_item_sk_node_13 ASC])
         +- Exchange(distribution=[hash[ws_item_sk_node_13]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(cd_purchase_estimate = inv_date_sk)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, cc_call_center_sk_node_12, cc_call_center_id_node_12, cc_rec_start_date_node_12, cc_rec_end_date_node_12, cc_closed_date_sk_node_12, cc_open_date_sk_node_12, cc_name_node_12, cc_class_node_12, cc_employees_node_12, cc_sq_ft_node_12, cc_hours_node_12, cc_manager_node_12, cc_mkt_id_node_12, cc_mkt_class_node_12, cc_mkt_desc_node_12, cc_market_manager_node_12, cc_division_node_12, cc_division_name_node_12, cc_company_node_12, cc_company_name_node_12, cc_street_number_node_12, cc_street_name_node_12, cc_street_type_node_12, cc_suite_number_node_12, cc_city_node_12, cc_county_node_12, cc_state_node_12, cc_zip_node_12, cc_country_node_12, cc_gmt_offset_node_12, cc_tax_percentage_node_12, ws_sold_date_sk_node_13, ws_sold_time_sk_node_13, ws_ship_date_sk_node_13, ws_item_sk_node_13, ws_bill_customer_sk_node_13, ws_bill_cdemo_sk_node_13, ws_bill_hdemo_sk_node_13, ws_bill_addr_sk_node_13, ws_ship_customer_sk_node_13, ws_ship_cdemo_sk_node_13, ws_ship_hdemo_sk_node_13, ws_ship_addr_sk_node_13, ws_web_page_sk_node_13, ws_web_site_sk_node_13, ws_ship_mode_sk_node_13, ws_warehouse_sk_node_13, ws_promo_sk_node_13, ws_order_number_node_13, ws_quantity_node_13, ws_wholesale_cost_node_13, ws_list_price_node_13, ws_sales_price_node_13, ws_ext_discount_amt_node_13, ws_ext_sales_price_node_13, ws_ext_wholesale_cost_node_13, ws_ext_list_price_node_13, ws_ext_tax_node_13, ws_coupon_amt_node_13, ws_ext_ship_cost_node_13, ws_net_paid_node_13, ws_net_paid_inc_tax_node_13, ws_net_paid_inc_ship_node_13, ws_net_paid_inc_ship_tax_node_13, ws_net_profit_node_13, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- SortLimit(orderBy=[cd_dep_college_count ASC], offset=[0], fetch=[1], global=[true])
               :     +- Exchange(distribution=[single])
               :        +- SortLimit(orderBy=[cd_dep_college_count ASC], offset=[0], fetch=[1], global=[false])
               :           +- HashAggregate(isMerge=[true], groupBy=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
               :              +- Exchange(distribution=[hash[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count]])
               :                 +- LocalHashAggregate(groupBy=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
               :                    +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
               +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ws_quantity_node_13 = inv_item_sk)], select=[cc_call_center_sk_node_12, cc_call_center_id_node_12, cc_rec_start_date_node_12, cc_rec_end_date_node_12, cc_closed_date_sk_node_12, cc_open_date_sk_node_12, cc_name_node_12, cc_class_node_12, cc_employees_node_12, cc_sq_ft_node_12, cc_hours_node_12, cc_manager_node_12, cc_mkt_id_node_12, cc_mkt_class_node_12, cc_mkt_desc_node_12, cc_market_manager_node_12, cc_division_node_12, cc_division_name_node_12, cc_company_node_12, cc_company_name_node_12, cc_street_number_node_12, cc_street_name_node_12, cc_street_type_node_12, cc_suite_number_node_12, cc_city_node_12, cc_county_node_12, cc_state_node_12, cc_zip_node_12, cc_country_node_12, cc_gmt_offset_node_12, cc_tax_percentage_node_12, ws_sold_date_sk_node_13, ws_sold_time_sk_node_13, ws_ship_date_sk_node_13, ws_item_sk_node_13, ws_bill_customer_sk_node_13, ws_bill_cdemo_sk_node_13, ws_bill_hdemo_sk_node_13, ws_bill_addr_sk_node_13, ws_ship_customer_sk_node_13, ws_ship_cdemo_sk_node_13, ws_ship_hdemo_sk_node_13, ws_ship_addr_sk_node_13, ws_web_page_sk_node_13, ws_web_site_sk_node_13, ws_ship_mode_sk_node_13, ws_warehouse_sk_node_13, ws_promo_sk_node_13, ws_order_number_node_13, ws_quantity_node_13, ws_wholesale_cost_node_13, ws_list_price_node_13, ws_sales_price_node_13, ws_ext_discount_amt_node_13, ws_ext_sales_price_node_13, ws_ext_wholesale_cost_node_13, ws_ext_list_price_node_13, ws_ext_tax_node_13, ws_coupon_amt_node_13, ws_ext_ship_cost_node_13, ws_net_paid_node_13, ws_net_paid_inc_tax_node_13, ws_net_paid_inc_ship_node_13, ws_net_paid_inc_ship_tax_node_13, ws_net_profit_node_13, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], build=[left])
                  :- Exchange(distribution=[hash[ws_quantity_node_13]])
                  :  +- Calc(select=[cc_call_center_sk_node_12, cc_call_center_id_node_12, cc_rec_start_date_node_12, cc_rec_end_date_node_12, cc_closed_date_sk_node_12, cc_open_date_sk_node_12, cc_name_node_12, cc_class_node_12, cc_employees_node_12, cc_sq_ft_node_12, cc_hours_node_12, cc_manager_node_12, cc_mkt_id_node_12, cc_mkt_class_node_12, cc_mkt_desc_node_12, cc_market_manager_node_12, cc_division_node_12, cc_division_name_node_12, cc_company_node_12, cc_company_name_node_12, cc_street_number_node_12, cc_street_name_node_12, cc_street_type_node_12, cc_suite_number_node_12, cc_city_node_12, cc_county_node_12, cc_state_node_12, cc_zip_node_12, cc_country_node_12, cc_gmt_offset_node_12, cc_tax_percentage_node_12, ws_sold_date_sk AS ws_sold_date_sk_node_13, ws_sold_time_sk AS ws_sold_time_sk_node_13, ws_ship_date_sk AS ws_ship_date_sk_node_13, ws_item_sk AS ws_item_sk_node_13, ws_bill_customer_sk AS ws_bill_customer_sk_node_13, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_13, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_13, ws_bill_addr_sk AS ws_bill_addr_sk_node_13, ws_ship_customer_sk AS ws_ship_customer_sk_node_13, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_13, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_13, ws_ship_addr_sk AS ws_ship_addr_sk_node_13, ws_web_page_sk AS ws_web_page_sk_node_13, ws_web_site_sk AS ws_web_site_sk_node_13, ws_ship_mode_sk AS ws_ship_mode_sk_node_13, ws_warehouse_sk AS ws_warehouse_sk_node_13, ws_promo_sk AS ws_promo_sk_node_13, ws_order_number AS ws_order_number_node_13, ws_quantity AS ws_quantity_node_13, ws_wholesale_cost AS ws_wholesale_cost_node_13, ws_list_price AS ws_list_price_node_13, ws_sales_price AS ws_sales_price_node_13, ws_ext_discount_amt AS ws_ext_discount_amt_node_13, ws_ext_sales_price AS ws_ext_sales_price_node_13, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_13, ws_ext_list_price AS ws_ext_list_price_node_13, ws_ext_tax AS ws_ext_tax_node_13, ws_coupon_amt AS ws_coupon_amt_node_13, ws_ext_ship_cost AS ws_ext_ship_cost_node_13, ws_net_paid AS ws_net_paid_node_13, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_13, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_13, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_13, ws_net_profit AS ws_net_profit_node_13])
                  :     +- NestedLoopJoin(joinType=[InnerJoin], where=[(cc_tax_percentage_node_120 = ws_ext_ship_cost)], select=[cc_call_center_sk_node_12, cc_call_center_id_node_12, cc_rec_start_date_node_12, cc_rec_end_date_node_12, cc_closed_date_sk_node_12, cc_open_date_sk_node_12, cc_name_node_12, cc_class_node_12, cc_employees_node_12, cc_sq_ft_node_12, cc_hours_node_12, cc_manager_node_12, cc_mkt_id_node_12, cc_mkt_class_node_12, cc_mkt_desc_node_12, cc_market_manager_node_12, cc_division_node_12, cc_division_name_node_12, cc_company_node_12, cc_company_name_node_12, cc_street_number_node_12, cc_street_name_node_12, cc_street_type_node_12, cc_suite_number_node_12, cc_city_node_12, cc_county_node_12, cc_state_node_12, cc_zip_node_12, cc_country_node_12, cc_gmt_offset_node_12, cc_tax_percentage_node_12, cc_tax_percentage_node_120, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
                  :        :- Exchange(distribution=[broadcast])
                  :        :  +- Calc(select=[cc_call_center_sk AS cc_call_center_sk_node_12, cc_call_center_id AS cc_call_center_id_node_12, cc_rec_start_date AS cc_rec_start_date_node_12, cc_rec_end_date AS cc_rec_end_date_node_12, cc_closed_date_sk AS cc_closed_date_sk_node_12, cc_open_date_sk AS cc_open_date_sk_node_12, cc_name AS cc_name_node_12, cc_class AS cc_class_node_12, cc_employees AS cc_employees_node_12, cc_sq_ft AS cc_sq_ft_node_12, cc_hours AS cc_hours_node_12, cc_manager AS cc_manager_node_12, cc_mkt_id AS cc_mkt_id_node_12, cc_mkt_class AS cc_mkt_class_node_12, cc_mkt_desc AS cc_mkt_desc_node_12, cc_market_manager AS cc_market_manager_node_12, cc_division AS cc_division_node_12, cc_division_name AS cc_division_name_node_12, cc_company AS cc_company_node_12, cc_company_name AS cc_company_name_node_12, cc_street_number AS cc_street_number_node_12, cc_street_name AS cc_street_name_node_12, cc_street_type AS cc_street_type_node_12, cc_suite_number AS cc_suite_number_node_12, cc_city AS cc_city_node_12, cc_county AS cc_county_node_12, cc_state AS cc_state_node_12, cc_zip AS cc_zip_node_12, cc_country AS cc_country_node_12, cc_gmt_offset AS cc_gmt_offset_node_12, cc_tax_percentage AS cc_tax_percentage_node_12, CAST(cc_tax_percentage AS DECIMAL(7, 2)) AS cc_tax_percentage_node_120])
                  :        :     +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
                  :        +- Calc(select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], where=[(ws_net_paid_inc_ship < 3.4403008222579956E1)])
                  :           +- TableSourceScan(table=[[default_catalog, default_database, web_sales, filter=[<(ws_net_paid_inc_ship, 3.4403008222579956E1:DOUBLE)]]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                  +- Exchange(distribution=[hash[inv_item_sk]])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(s_hours = web_suite_number)], select=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], build=[right])
                        :- HashJoin(joinType=[InnerJoin], where=[(inv_warehouse_sk = s_closed_date_sk)], select=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], isBroadcast=[true], build=[right])
                        :  :- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
                        :  +- Exchange(distribution=[broadcast])
                        :     +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                        +- Exchange(distribution=[broadcast])
                           +- SortLimit(orderBy=[web_city ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[web_city ASC], offset=[0], fetch=[1], global=[false])
                                    +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0