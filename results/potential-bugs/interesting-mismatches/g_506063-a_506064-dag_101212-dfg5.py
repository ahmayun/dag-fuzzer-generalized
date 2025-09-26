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

autonode_13 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_12 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_14 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_15 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_10 = autonode_13.filter(col('cs_bill_customer_sk_node_13') > 10)
autonode_9 = autonode_12.alias('O0nEv')
autonode_11 = autonode_14.join(autonode_15, col('cd_dep_college_count_node_14') == col('cs_sold_time_sk_node_15'))
autonode_7 = autonode_10.order_by(col('cs_ship_mode_sk_node_13'))
autonode_6 = autonode_9.order_by(col('i_units_node_12'))
autonode_8 = autonode_11.alias('2x5LM')
autonode_4 = autonode_6.join(autonode_7, col('cs_ext_list_price_node_13') == col('i_wholesale_cost_node_12'))
autonode_5 = autonode_8.alias('6hSzO')
autonode_2 = autonode_4.group_by(col('i_manager_id_node_12')).select(col('i_brand_id_node_12').max.alias('i_brand_id_node_12'))
autonode_3 = autonode_5.limit(90)
autonode_1 = autonode_2.join(autonode_3, col('i_brand_id_node_12') == col('cs_ship_addr_sk_node_15'))
sink = autonode_1.order_by(col('cs_net_paid_node_15'))
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
      "error_message": "An error occurred while calling o275556848.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#557490922:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[18](input=RelSubset#557490920,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[18]), rel#557490919:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[18](input=RelSubset#557490918,groupBy=i_wholesale_cost, i_manager_id,select=i_wholesale_cost, i_manager_id, Partial_MAX(i_brand_id) AS max$0)]
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
LogicalSort(sort0=[$39], dir0=[ASC])
+- LogicalJoin(condition=[=($0, $20)], joinType=[inner])
   :- LogicalProject(i_brand_id_node_12=[$1])
   :  +- LogicalAggregate(group=[{20}], EXPR$0=[MAX($7)])
   :     +- LogicalJoin(condition=[=($47, $6)], joinType=[inner])
   :        :- LogicalSort(sort0=[$18], dir0=[ASC])
   :        :  +- LogicalProject(O0nEv=[AS($0, _UTF-16LE'O0nEv')], i_item_id_node_12=[$1], i_rec_start_date_node_12=[$2], i_rec_end_date_node_12=[$3], i_item_desc_node_12=[$4], i_current_price_node_12=[$5], i_wholesale_cost_node_12=[$6], i_brand_id_node_12=[$7], i_brand_node_12=[$8], i_class_id_node_12=[$9], i_class_node_12=[$10], i_category_id_node_12=[$11], i_category_node_12=[$12], i_manufact_id_node_12=[$13], i_manufact_node_12=[$14], i_size_node_12=[$15], i_formulation_node_12=[$16], i_color_node_12=[$17], i_units_node_12=[$18], i_container_node_12=[$19], i_manager_id_node_12=[$20], i_product_name_node_12=[$21])
   :        :     +- LogicalTableScan(table=[[default_catalog, default_database, item]])
   :        +- LogicalSort(sort0=[$13], dir0=[ASC])
   :           +- LogicalFilter(condition=[>($3, 10)])
   :              +- LogicalProject(cs_sold_date_sk_node_13=[$0], cs_sold_time_sk_node_13=[$1], cs_ship_date_sk_node_13=[$2], cs_bill_customer_sk_node_13=[$3], cs_bill_cdemo_sk_node_13=[$4], cs_bill_hdemo_sk_node_13=[$5], cs_bill_addr_sk_node_13=[$6], cs_ship_customer_sk_node_13=[$7], cs_ship_cdemo_sk_node_13=[$8], cs_ship_hdemo_sk_node_13=[$9], cs_ship_addr_sk_node_13=[$10], cs_call_center_sk_node_13=[$11], cs_catalog_page_sk_node_13=[$12], cs_ship_mode_sk_node_13=[$13], cs_warehouse_sk_node_13=[$14], cs_item_sk_node_13=[$15], cs_promo_sk_node_13=[$16], cs_order_number_node_13=[$17], cs_quantity_node_13=[$18], cs_wholesale_cost_node_13=[$19], cs_list_price_node_13=[$20], cs_sales_price_node_13=[$21], cs_ext_discount_amt_node_13=[$22], cs_ext_sales_price_node_13=[$23], cs_ext_wholesale_cost_node_13=[$24], cs_ext_list_price_node_13=[$25], cs_ext_tax_node_13=[$26], cs_coupon_amt_node_13=[$27], cs_ext_ship_cost_node_13=[$28], cs_net_paid_node_13=[$29], cs_net_paid_inc_tax_node_13=[$30], cs_net_paid_inc_ship_node_13=[$31], cs_net_paid_inc_ship_tax_node_13=[$32], cs_net_profit_node_13=[$33])
   :                 +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
   +- LogicalSort(fetch=[90])
      +- LogicalProject(6hSzO=[AS(AS($0, _UTF-16LE'2x5LM'), _UTF-16LE'6hSzO')], cd_gender_node_14=[$1], cd_marital_status_node_14=[$2], cd_education_status_node_14=[$3], cd_purchase_estimate_node_14=[$4], cd_credit_rating_node_14=[$5], cd_dep_count_node_14=[$6], cd_dep_employed_count_node_14=[$7], cd_dep_college_count_node_14=[$8], cs_sold_date_sk_node_15=[$9], cs_sold_time_sk_node_15=[$10], cs_ship_date_sk_node_15=[$11], cs_bill_customer_sk_node_15=[$12], cs_bill_cdemo_sk_node_15=[$13], cs_bill_hdemo_sk_node_15=[$14], cs_bill_addr_sk_node_15=[$15], cs_ship_customer_sk_node_15=[$16], cs_ship_cdemo_sk_node_15=[$17], cs_ship_hdemo_sk_node_15=[$18], cs_ship_addr_sk_node_15=[$19], cs_call_center_sk_node_15=[$20], cs_catalog_page_sk_node_15=[$21], cs_ship_mode_sk_node_15=[$22], cs_warehouse_sk_node_15=[$23], cs_item_sk_node_15=[$24], cs_promo_sk_node_15=[$25], cs_order_number_node_15=[$26], cs_quantity_node_15=[$27], cs_wholesale_cost_node_15=[$28], cs_list_price_node_15=[$29], cs_sales_price_node_15=[$30], cs_ext_discount_amt_node_15=[$31], cs_ext_sales_price_node_15=[$32], cs_ext_wholesale_cost_node_15=[$33], cs_ext_list_price_node_15=[$34], cs_ext_tax_node_15=[$35], cs_coupon_amt_node_15=[$36], cs_ext_ship_cost_node_15=[$37], cs_net_paid_node_15=[$38], cs_net_paid_inc_tax_node_15=[$39], cs_net_paid_inc_ship_node_15=[$40], cs_net_paid_inc_ship_tax_node_15=[$41], cs_net_profit_node_15=[$42])
         +- LogicalJoin(condition=[=($8, $10)], joinType=[inner])
            :- LogicalProject(cd_demo_sk_node_14=[$0], cd_gender_node_14=[$1], cd_marital_status_node_14=[$2], cd_education_status_node_14=[$3], cd_purchase_estimate_node_14=[$4], cd_credit_rating_node_14=[$5], cd_dep_count_node_14=[$6], cd_dep_employed_count_node_14=[$7], cd_dep_college_count_node_14=[$8])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
            +- LogicalProject(cs_sold_date_sk_node_15=[$0], cs_sold_time_sk_node_15=[$1], cs_ship_date_sk_node_15=[$2], cs_bill_customer_sk_node_15=[$3], cs_bill_cdemo_sk_node_15=[$4], cs_bill_hdemo_sk_node_15=[$5], cs_bill_addr_sk_node_15=[$6], cs_ship_customer_sk_node_15=[$7], cs_ship_cdemo_sk_node_15=[$8], cs_ship_hdemo_sk_node_15=[$9], cs_ship_addr_sk_node_15=[$10], cs_call_center_sk_node_15=[$11], cs_catalog_page_sk_node_15=[$12], cs_ship_mode_sk_node_15=[$13], cs_warehouse_sk_node_15=[$14], cs_item_sk_node_15=[$15], cs_promo_sk_node_15=[$16], cs_order_number_node_15=[$17], cs_quantity_node_15=[$18], cs_wholesale_cost_node_15=[$19], cs_list_price_node_15=[$20], cs_sales_price_node_15=[$21], cs_ext_discount_amt_node_15=[$22], cs_ext_sales_price_node_15=[$23], cs_ext_wholesale_cost_node_15=[$24], cs_ext_list_price_node_15=[$25], cs_ext_tax_node_15=[$26], cs_coupon_amt_node_15=[$27], cs_ext_ship_cost_node_15=[$28], cs_net_paid_node_15=[$29], cs_net_paid_inc_tax_node_15=[$30], cs_net_paid_inc_ship_node_15=[$31], cs_net_paid_inc_ship_tax_node_15=[$32], cs_net_profit_node_15=[$33])
               +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])

== Optimized Physical Plan ==
SortLimit(orderBy=[cs_net_paid_node_15 ASC], offset=[0], fetch=[1], global=[true])
+- Exchange(distribution=[single])
   +- SortLimit(orderBy=[cs_net_paid_node_15 ASC], offset=[0], fetch=[1], global=[false])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(i_brand_id_node_12, cs_ship_addr_sk_node_15)], select=[i_brand_id_node_12, 6hSzO, cd_gender_node_14, cd_marital_status_node_14, cd_education_status_node_14, cd_purchase_estimate_node_14, cd_credit_rating_node_14, cd_dep_count_node_14, cd_dep_employed_count_node_14, cd_dep_college_count_node_14, cs_sold_date_sk_node_15, cs_sold_time_sk_node_15, cs_ship_date_sk_node_15, cs_bill_customer_sk_node_15, cs_bill_cdemo_sk_node_15, cs_bill_hdemo_sk_node_15, cs_bill_addr_sk_node_15, cs_ship_customer_sk_node_15, cs_ship_cdemo_sk_node_15, cs_ship_hdemo_sk_node_15, cs_ship_addr_sk_node_15, cs_call_center_sk_node_15, cs_catalog_page_sk_node_15, cs_ship_mode_sk_node_15, cs_warehouse_sk_node_15, cs_item_sk_node_15, cs_promo_sk_node_15, cs_order_number_node_15, cs_quantity_node_15, cs_wholesale_cost_node_15, cs_list_price_node_15, cs_sales_price_node_15, cs_ext_discount_amt_node_15, cs_ext_sales_price_node_15, cs_ext_wholesale_cost_node_15, cs_ext_list_price_node_15, cs_ext_tax_node_15, cs_coupon_amt_node_15, cs_ext_ship_cost_node_15, cs_net_paid_node_15, cs_net_paid_inc_tax_node_15, cs_net_paid_inc_ship_node_15, cs_net_paid_inc_ship_tax_node_15, cs_net_profit_node_15], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[EXPR$0 AS i_brand_id_node_12])
         :     +- SortAggregate(isMerge=[false], groupBy=[i_manager_id_node_12], select=[i_manager_id_node_12, MAX(i_brand_id_node_12) AS EXPR$0])
         :        +- Sort(orderBy=[i_manager_id_node_12 ASC])
         :           +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cs_ext_list_price, i_wholesale_cost_node_12)], select=[O0nEv, i_item_id_node_12, i_rec_start_date_node_12, i_rec_end_date_node_12, i_item_desc_node_12, i_current_price_node_12, i_wholesale_cost_node_12, i_brand_id_node_12, i_brand_node_12, i_class_id_node_12, i_class_node_12, i_category_id_node_12, i_category_node_12, i_manufact_id_node_12, i_manufact_node_12, i_size_node_12, i_formulation_node_12, i_color_node_12, i_units_node_12, i_container_node_12, i_manager_id_node_12, i_product_name_node_12, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], build=[right])
         :              :- Exchange(distribution=[hash[i_manager_id_node_12]])
         :              :  +- Calc(select=[i_item_sk AS O0nEv, i_item_id AS i_item_id_node_12, i_rec_start_date AS i_rec_start_date_node_12, i_rec_end_date AS i_rec_end_date_node_12, i_item_desc AS i_item_desc_node_12, i_current_price AS i_current_price_node_12, i_wholesale_cost AS i_wholesale_cost_node_12, i_brand_id AS i_brand_id_node_12, i_brand AS i_brand_node_12, i_class_id AS i_class_id_node_12, i_class AS i_class_node_12, i_category_id AS i_category_id_node_12, i_category AS i_category_node_12, i_manufact_id AS i_manufact_id_node_12, i_manufact AS i_manufact_node_12, i_size AS i_size_node_12, i_formulation AS i_formulation_node_12, i_color AS i_color_node_12, i_units AS i_units_node_12, i_container AS i_container_node_12, i_manager_id AS i_manager_id_node_12, i_product_name AS i_product_name_node_12])
         :              :     +- SortLimit(orderBy=[i_units ASC], offset=[0], fetch=[1], global=[true])
         :              :        +- Exchange(distribution=[single])
         :              :           +- SortLimit(orderBy=[i_units ASC], offset=[0], fetch=[1], global=[false])
         :              :              +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
         :              +- Exchange(distribution=[broadcast])
         :                 +- SortLimit(orderBy=[cs_ship_mode_sk ASC], offset=[0], fetch=[1], global=[true])
         :                    +- Exchange(distribution=[single])
         :                       +- SortLimit(orderBy=[cs_ship_mode_sk ASC], offset=[0], fetch=[1], global=[false])
         :                          +- Calc(select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], where=[>(cs_bill_customer_sk, 10)])
         :                             +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales, filter=[>(cs_bill_customer_sk, 10)]]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
         +- Calc(select=[cd_demo_sk AS 6hSzO, cd_gender AS cd_gender_node_14, cd_marital_status AS cd_marital_status_node_14, cd_education_status AS cd_education_status_node_14, cd_purchase_estimate AS cd_purchase_estimate_node_14, cd_credit_rating AS cd_credit_rating_node_14, cd_dep_count AS cd_dep_count_node_14, cd_dep_employed_count AS cd_dep_employed_count_node_14, cd_dep_college_count AS cd_dep_college_count_node_14, cs_sold_date_sk AS cs_sold_date_sk_node_15, cs_sold_time_sk AS cs_sold_time_sk_node_15, cs_ship_date_sk AS cs_ship_date_sk_node_15, cs_bill_customer_sk AS cs_bill_customer_sk_node_15, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_15, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_15, cs_bill_addr_sk AS cs_bill_addr_sk_node_15, cs_ship_customer_sk AS cs_ship_customer_sk_node_15, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_15, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_15, cs_ship_addr_sk AS cs_ship_addr_sk_node_15, cs_call_center_sk AS cs_call_center_sk_node_15, cs_catalog_page_sk AS cs_catalog_page_sk_node_15, cs_ship_mode_sk AS cs_ship_mode_sk_node_15, cs_warehouse_sk AS cs_warehouse_sk_node_15, cs_item_sk AS cs_item_sk_node_15, cs_promo_sk AS cs_promo_sk_node_15, cs_order_number AS cs_order_number_node_15, cs_quantity AS cs_quantity_node_15, cs_wholesale_cost AS cs_wholesale_cost_node_15, cs_list_price AS cs_list_price_node_15, cs_sales_price AS cs_sales_price_node_15, cs_ext_discount_amt AS cs_ext_discount_amt_node_15, cs_ext_sales_price AS cs_ext_sales_price_node_15, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_15, cs_ext_list_price AS cs_ext_list_price_node_15, cs_ext_tax AS cs_ext_tax_node_15, cs_coupon_amt AS cs_coupon_amt_node_15, cs_ext_ship_cost AS cs_ext_ship_cost_node_15, cs_net_paid AS cs_net_paid_node_15, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_15, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_15, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_15, cs_net_profit AS cs_net_profit_node_15])
            +- Limit(offset=[0], fetch=[90], global=[true])
               +- Exchange(distribution=[single])
                  +- Limit(offset=[0], fetch=[90], global=[false])
                     +- HashJoin(joinType=[InnerJoin], where=[=(cd_dep_college_count, cs_sold_time_sk)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], build=[left])
                        :- Exchange(distribution=[hash[cd_dep_college_count]])
                        :  +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                        +- Exchange(distribution=[hash[cs_sold_time_sk]])
                           +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

== Optimized Execution Plan ==
SortLimit(orderBy=[cs_net_paid_node_15 ASC], offset=[0], fetch=[1], global=[true])
+- Exchange(distribution=[single])
   +- SortLimit(orderBy=[cs_net_paid_node_15 ASC], offset=[0], fetch=[1], global=[false])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[(i_brand_id_node_12 = cs_ship_addr_sk_node_15)], select=[i_brand_id_node_12, 6hSzO, cd_gender_node_14, cd_marital_status_node_14, cd_education_status_node_14, cd_purchase_estimate_node_14, cd_credit_rating_node_14, cd_dep_count_node_14, cd_dep_employed_count_node_14, cd_dep_college_count_node_14, cs_sold_date_sk_node_15, cs_sold_time_sk_node_15, cs_ship_date_sk_node_15, cs_bill_customer_sk_node_15, cs_bill_cdemo_sk_node_15, cs_bill_hdemo_sk_node_15, cs_bill_addr_sk_node_15, cs_ship_customer_sk_node_15, cs_ship_cdemo_sk_node_15, cs_ship_hdemo_sk_node_15, cs_ship_addr_sk_node_15, cs_call_center_sk_node_15, cs_catalog_page_sk_node_15, cs_ship_mode_sk_node_15, cs_warehouse_sk_node_15, cs_item_sk_node_15, cs_promo_sk_node_15, cs_order_number_node_15, cs_quantity_node_15, cs_wholesale_cost_node_15, cs_list_price_node_15, cs_sales_price_node_15, cs_ext_discount_amt_node_15, cs_ext_sales_price_node_15, cs_ext_wholesale_cost_node_15, cs_ext_list_price_node_15, cs_ext_tax_node_15, cs_coupon_amt_node_15, cs_ext_ship_cost_node_15, cs_net_paid_node_15, cs_net_paid_inc_tax_node_15, cs_net_paid_inc_ship_node_15, cs_net_paid_inc_ship_tax_node_15, cs_net_profit_node_15], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[EXPR$0 AS i_brand_id_node_12])
         :     +- SortAggregate(isMerge=[false], groupBy=[i_manager_id_node_12], select=[i_manager_id_node_12, MAX(i_brand_id_node_12) AS EXPR$0])
         :        +- Exchange(distribution=[forward])
         :           +- Sort(orderBy=[i_manager_id_node_12 ASC])
         :              +- Exchange(distribution=[keep_input_as_is[hash[i_manager_id_node_12]]])
         :                 +- NestedLoopJoin(joinType=[InnerJoin], where=[(cs_ext_list_price = i_wholesale_cost_node_12)], select=[O0nEv, i_item_id_node_12, i_rec_start_date_node_12, i_rec_end_date_node_12, i_item_desc_node_12, i_current_price_node_12, i_wholesale_cost_node_12, i_brand_id_node_12, i_brand_node_12, i_class_id_node_12, i_class_node_12, i_category_id_node_12, i_category_node_12, i_manufact_id_node_12, i_manufact_node_12, i_size_node_12, i_formulation_node_12, i_color_node_12, i_units_node_12, i_container_node_12, i_manager_id_node_12, i_product_name_node_12, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], build=[right])
         :                    :- Exchange(distribution=[hash[i_manager_id_node_12]])
         :                    :  +- Calc(select=[i_item_sk AS O0nEv, i_item_id AS i_item_id_node_12, i_rec_start_date AS i_rec_start_date_node_12, i_rec_end_date AS i_rec_end_date_node_12, i_item_desc AS i_item_desc_node_12, i_current_price AS i_current_price_node_12, i_wholesale_cost AS i_wholesale_cost_node_12, i_brand_id AS i_brand_id_node_12, i_brand AS i_brand_node_12, i_class_id AS i_class_id_node_12, i_class AS i_class_node_12, i_category_id AS i_category_id_node_12, i_category AS i_category_node_12, i_manufact_id AS i_manufact_id_node_12, i_manufact AS i_manufact_node_12, i_size AS i_size_node_12, i_formulation AS i_formulation_node_12, i_color AS i_color_node_12, i_units AS i_units_node_12, i_container AS i_container_node_12, i_manager_id AS i_manager_id_node_12, i_product_name AS i_product_name_node_12])
         :                    :     +- SortLimit(orderBy=[i_units ASC], offset=[0], fetch=[1], global=[true])
         :                    :        +- Exchange(distribution=[single])
         :                    :           +- SortLimit(orderBy=[i_units ASC], offset=[0], fetch=[1], global=[false])
         :                    :              +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
         :                    +- Exchange(distribution=[broadcast])
         :                       +- SortLimit(orderBy=[cs_ship_mode_sk ASC], offset=[0], fetch=[1], global=[true])
         :                          +- Exchange(distribution=[single])
         :                             +- SortLimit(orderBy=[cs_ship_mode_sk ASC], offset=[0], fetch=[1], global=[false])
         :                                +- Calc(select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], where=[(cs_bill_customer_sk > 10)])
         :                                   +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales, filter=[>(cs_bill_customer_sk, 10)]]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
         +- Calc(select=[cd_demo_sk AS 6hSzO, cd_gender AS cd_gender_node_14, cd_marital_status AS cd_marital_status_node_14, cd_education_status AS cd_education_status_node_14, cd_purchase_estimate AS cd_purchase_estimate_node_14, cd_credit_rating AS cd_credit_rating_node_14, cd_dep_count AS cd_dep_count_node_14, cd_dep_employed_count AS cd_dep_employed_count_node_14, cd_dep_college_count AS cd_dep_college_count_node_14, cs_sold_date_sk AS cs_sold_date_sk_node_15, cs_sold_time_sk AS cs_sold_time_sk_node_15, cs_ship_date_sk AS cs_ship_date_sk_node_15, cs_bill_customer_sk AS cs_bill_customer_sk_node_15, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_15, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_15, cs_bill_addr_sk AS cs_bill_addr_sk_node_15, cs_ship_customer_sk AS cs_ship_customer_sk_node_15, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_15, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_15, cs_ship_addr_sk AS cs_ship_addr_sk_node_15, cs_call_center_sk AS cs_call_center_sk_node_15, cs_catalog_page_sk AS cs_catalog_page_sk_node_15, cs_ship_mode_sk AS cs_ship_mode_sk_node_15, cs_warehouse_sk AS cs_warehouse_sk_node_15, cs_item_sk AS cs_item_sk_node_15, cs_promo_sk AS cs_promo_sk_node_15, cs_order_number AS cs_order_number_node_15, cs_quantity AS cs_quantity_node_15, cs_wholesale_cost AS cs_wholesale_cost_node_15, cs_list_price AS cs_list_price_node_15, cs_sales_price AS cs_sales_price_node_15, cs_ext_discount_amt AS cs_ext_discount_amt_node_15, cs_ext_sales_price AS cs_ext_sales_price_node_15, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_15, cs_ext_list_price AS cs_ext_list_price_node_15, cs_ext_tax AS cs_ext_tax_node_15, cs_coupon_amt AS cs_coupon_amt_node_15, cs_ext_ship_cost AS cs_ext_ship_cost_node_15, cs_net_paid AS cs_net_paid_node_15, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_15, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_15, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_15, cs_net_profit AS cs_net_profit_node_15])
            +- Limit(offset=[0], fetch=[90], global=[true])
               +- Exchange(distribution=[single])
                  +- Limit(offset=[0], fetch=[90], global=[false])
                     +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(cd_dep_college_count = cs_sold_time_sk)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], build=[left])
                        :- Exchange(distribution=[hash[cd_dep_college_count]])
                        :  +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                        +- Exchange(distribution=[hash[cs_sold_time_sk]])
                           +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0