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
    return values.kurtosis()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_8 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_7 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_4 = autonode_6.add_columns(lit("hello"))
autonode_5 = autonode_7.join(autonode_8, col('cr_refunded_addr_sk_node_7') == col('w_warehouse_sq_ft_node_8'))
autonode_2 = autonode_4.order_by(col('i_units_node_6'))
autonode_3 = autonode_5.group_by(col('cr_returned_date_sk_node_7')).select(col('cr_returning_cdemo_sk_node_7').max.alias('cr_returning_cdemo_sk_node_7'))
autonode_1 = autonode_2.join(autonode_3, col('i_brand_id_node_6') == col('cr_returning_cdemo_sk_node_7'))
sink = autonode_1.group_by(col('i_size_node_6')).select(col('i_current_price_node_6').max.alias('i_current_price_node_6'))
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
LogicalProject(i_current_price_node_6=[$1])
+- LogicalAggregate(group=[{15}], EXPR$0=[MAX($5)])
   +- LogicalJoin(condition=[=($7, $23)], joinType=[inner])
      :- LogicalSort(sort0=[$18], dir0=[ASC])
      :  +- LogicalProject(i_item_sk_node_6=[$0], i_item_id_node_6=[$1], i_rec_start_date_node_6=[$2], i_rec_end_date_node_6=[$3], i_item_desc_node_6=[$4], i_current_price_node_6=[$5], i_wholesale_cost_node_6=[$6], i_brand_id_node_6=[$7], i_brand_node_6=[$8], i_class_id_node_6=[$9], i_class_node_6=[$10], i_category_id_node_6=[$11], i_category_node_6=[$12], i_manufact_id_node_6=[$13], i_manufact_node_6=[$14], i_size_node_6=[$15], i_formulation_node_6=[$16], i_color_node_6=[$17], i_units_node_6=[$18], i_container_node_6=[$19], i_manager_id_node_6=[$20], i_product_name_node_6=[$21], _c22=[_UTF-16LE'hello'])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, item]])
      +- LogicalProject(cr_returning_cdemo_sk_node_7=[$1])
         +- LogicalAggregate(group=[{0}], EXPR$0=[MAX($8)])
            +- LogicalJoin(condition=[=($6, $30)], joinType=[inner])
               :- LogicalProject(cr_returned_date_sk_node_7=[$0], cr_returned_time_sk_node_7=[$1], cr_item_sk_node_7=[$2], cr_refunded_customer_sk_node_7=[$3], cr_refunded_cdemo_sk_node_7=[$4], cr_refunded_hdemo_sk_node_7=[$5], cr_refunded_addr_sk_node_7=[$6], cr_returning_customer_sk_node_7=[$7], cr_returning_cdemo_sk_node_7=[$8], cr_returning_hdemo_sk_node_7=[$9], cr_returning_addr_sk_node_7=[$10], cr_call_center_sk_node_7=[$11], cr_catalog_page_sk_node_7=[$12], cr_ship_mode_sk_node_7=[$13], cr_warehouse_sk_node_7=[$14], cr_reason_sk_node_7=[$15], cr_order_number_node_7=[$16], cr_return_quantity_node_7=[$17], cr_return_amount_node_7=[$18], cr_return_tax_node_7=[$19], cr_return_amt_inc_tax_node_7=[$20], cr_fee_node_7=[$21], cr_return_ship_cost_node_7=[$22], cr_refunded_cash_node_7=[$23], cr_reversed_charge_node_7=[$24], cr_store_credit_node_7=[$25], cr_net_loss_node_7=[$26])
               :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
               +- LogicalProject(w_warehouse_sk_node_8=[$0], w_warehouse_id_node_8=[$1], w_warehouse_name_node_8=[$2], w_warehouse_sq_ft_node_8=[$3], w_street_number_node_8=[$4], w_street_name_node_8=[$5], w_street_type_node_8=[$6], w_suite_number_node_8=[$7], w_city_node_8=[$8], w_county_node_8=[$9], w_state_node_8=[$10], w_zip_node_8=[$11], w_country_node_8=[$12], w_gmt_offset_node_8=[$13])
                  +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS i_current_price_node_6])
+- HashAggregate(isMerge=[true], groupBy=[i_size_node_6], select=[i_size_node_6, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[i_size_node_6]])
      +- LocalHashAggregate(groupBy=[i_size_node_6], select=[i_size_node_6, Partial_MAX(i_current_price_node_6) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(i_brand_id_node_6, cr_returning_cdemo_sk_node_7)], select=[i_item_sk_node_6, i_item_id_node_6, i_rec_start_date_node_6, i_rec_end_date_node_6, i_item_desc_node_6, i_current_price_node_6, i_wholesale_cost_node_6, i_brand_id_node_6, i_brand_node_6, i_class_id_node_6, i_class_node_6, i_category_id_node_6, i_category_node_6, i_manufact_id_node_6, i_manufact_node_6, i_size_node_6, i_formulation_node_6, i_color_node_6, i_units_node_6, i_container_node_6, i_manager_id_node_6, i_product_name_node_6, _c22, cr_returning_cdemo_sk_node_7], isBroadcast=[true], build=[right])
            :- Sort(orderBy=[i_units_node_6 ASC])
            :  +- Calc(select=[i_item_sk AS i_item_sk_node_6, i_item_id AS i_item_id_node_6, i_rec_start_date AS i_rec_start_date_node_6, i_rec_end_date AS i_rec_end_date_node_6, i_item_desc AS i_item_desc_node_6, i_current_price AS i_current_price_node_6, i_wholesale_cost AS i_wholesale_cost_node_6, i_brand_id AS i_brand_id_node_6, i_brand AS i_brand_node_6, i_class_id AS i_class_id_node_6, i_class AS i_class_node_6, i_category_id AS i_category_id_node_6, i_category AS i_category_node_6, i_manufact_id AS i_manufact_id_node_6, i_manufact AS i_manufact_node_6, i_size AS i_size_node_6, i_formulation AS i_formulation_node_6, i_color AS i_color_node_6, i_units AS i_units_node_6, i_container AS i_container_node_6, i_manager_id AS i_manager_id_node_6, i_product_name AS i_product_name_node_6, 'hello' AS _c22])
            :     +- Exchange(distribution=[single])
            :        +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[EXPR$0 AS cr_returning_cdemo_sk_node_7])
                  +- HashAggregate(isMerge=[true], groupBy=[cr_returned_date_sk], select=[cr_returned_date_sk, Final_MAX(max$0) AS EXPR$0])
                     +- Exchange(distribution=[hash[cr_returned_date_sk]])
                        +- LocalHashAggregate(groupBy=[cr_returned_date_sk], select=[cr_returned_date_sk, Partial_MAX(cr_returning_cdemo_sk) AS max$0])
                           +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cr_refunded_addr_sk, w_warehouse_sq_ft)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], build=[right])
                              :- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                              +- Exchange(distribution=[broadcast])
                                 +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS i_current_price_node_6])
+- HashAggregate(isMerge=[true], groupBy=[i_size_node_6], select=[i_size_node_6, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[i_size_node_6]])
      +- LocalHashAggregate(groupBy=[i_size_node_6], select=[i_size_node_6, Partial_MAX(i_current_price_node_6) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[(i_brand_id_node_6 = cr_returning_cdemo_sk_node_7)], select=[i_item_sk_node_6, i_item_id_node_6, i_rec_start_date_node_6, i_rec_end_date_node_6, i_item_desc_node_6, i_current_price_node_6, i_wholesale_cost_node_6, i_brand_id_node_6, i_brand_node_6, i_class_id_node_6, i_class_node_6, i_category_id_node_6, i_category_node_6, i_manufact_id_node_6, i_manufact_node_6, i_size_node_6, i_formulation_node_6, i_color_node_6, i_units_node_6, i_container_node_6, i_manager_id_node_6, i_product_name_node_6, _c22, cr_returning_cdemo_sk_node_7], isBroadcast=[true], build=[right])
            :- Sort(orderBy=[i_units_node_6 ASC])
            :  +- Calc(select=[i_item_sk AS i_item_sk_node_6, i_item_id AS i_item_id_node_6, i_rec_start_date AS i_rec_start_date_node_6, i_rec_end_date AS i_rec_end_date_node_6, i_item_desc AS i_item_desc_node_6, i_current_price AS i_current_price_node_6, i_wholesale_cost AS i_wholesale_cost_node_6, i_brand_id AS i_brand_id_node_6, i_brand AS i_brand_node_6, i_class_id AS i_class_id_node_6, i_class AS i_class_node_6, i_category_id AS i_category_id_node_6, i_category AS i_category_node_6, i_manufact_id AS i_manufact_id_node_6, i_manufact AS i_manufact_node_6, i_size AS i_size_node_6, i_formulation AS i_formulation_node_6, i_color AS i_color_node_6, i_units AS i_units_node_6, i_container AS i_container_node_6, i_manager_id AS i_manager_id_node_6, i_product_name AS i_product_name_node_6, 'hello' AS _c22])
            :     +- Exchange(distribution=[single])
            :        +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[EXPR$0 AS cr_returning_cdemo_sk_node_7])
                  +- HashAggregate(isMerge=[true], groupBy=[cr_returned_date_sk], select=[cr_returned_date_sk, Final_MAX(max$0) AS EXPR$0])
                     +- Exchange(distribution=[hash[cr_returned_date_sk]])
                        +- LocalHashAggregate(groupBy=[cr_returned_date_sk], select=[cr_returned_date_sk, Partial_MAX(cr_returning_cdemo_sk) AS max$0])
                           +- MultipleInput(readOrder=[1,0], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(cr_refunded_addr_sk = w_warehouse_sq_ft)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])\
+- [#2] Exchange(distribution=[broadcast])\
])
                              :- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                              +- Exchange(distribution=[broadcast])
                                 +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o248189200.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#502225564:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[18](input=RelSubset#502225562,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[18]), rel#502225561:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[18](input=RelSubset#502225560,groupBy=i_brand_id, i_size,select=i_brand_id, i_size, Partial_MAX(i_current_price) AS max$0)]
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