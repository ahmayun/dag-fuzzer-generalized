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

autonode_6 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_7 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_4 = autonode_6.order_by(col('ss_sales_price_node_6'))
autonode_5 = autonode_7.alias('H1yMV')
autonode_2 = autonode_4.filter(col('ss_sold_date_sk_node_6') <= 23)
autonode_3 = autonode_5.filter(col('i_size_node_7').char_length <= 5)
autonode_1 = autonode_2.join(autonode_3, col('ss_ext_discount_amt_node_6') == col('i_current_price_node_7'))
sink = autonode_1.group_by(col('i_manager_id_node_7')).select(col('i_brand_id_node_7').min.alias('i_brand_id_node_7'))
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
      "error_message": "An error occurred while calling o312628349.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#629913296:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[13](input=RelSubset#629913294,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[13]), rel#629913293:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[13](input=RelSubset#629913292,groupBy=ss_ext_discount_amt,select=ss_ext_discount_amt)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (13) must be less than size (1)
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
LogicalProject(i_brand_id_node_7=[$1])
+- LogicalAggregate(group=[{43}], EXPR$0=[MIN($30)])
   +- LogicalJoin(condition=[=($14, $28)], joinType=[inner])
      :- LogicalFilter(condition=[<=($0, 23)])
      :  +- LogicalSort(sort0=[$13], dir0=[ASC])
      :     +- LogicalProject(ss_sold_date_sk_node_6=[$0], ss_sold_time_sk_node_6=[$1], ss_item_sk_node_6=[$2], ss_customer_sk_node_6=[$3], ss_cdemo_sk_node_6=[$4], ss_hdemo_sk_node_6=[$5], ss_addr_sk_node_6=[$6], ss_store_sk_node_6=[$7], ss_promo_sk_node_6=[$8], ss_ticket_number_node_6=[$9], ss_quantity_node_6=[$10], ss_wholesale_cost_node_6=[$11], ss_list_price_node_6=[$12], ss_sales_price_node_6=[$13], ss_ext_discount_amt_node_6=[$14], ss_ext_sales_price_node_6=[$15], ss_ext_wholesale_cost_node_6=[$16], ss_ext_list_price_node_6=[$17], ss_ext_tax_node_6=[$18], ss_coupon_amt_node_6=[$19], ss_net_paid_node_6=[$20], ss_net_paid_inc_tax_node_6=[$21], ss_net_profit_node_6=[$22])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalFilter(condition=[<=(CHAR_LENGTH($15), 5)])
         +- LogicalProject(H1yMV=[AS($0, _UTF-16LE'H1yMV')], i_item_id_node_7=[$1], i_rec_start_date_node_7=[$2], i_rec_end_date_node_7=[$3], i_item_desc_node_7=[$4], i_current_price_node_7=[$5], i_wholesale_cost_node_7=[$6], i_brand_id_node_7=[$7], i_brand_node_7=[$8], i_class_id_node_7=[$9], i_class_node_7=[$10], i_category_id_node_7=[$11], i_category_node_7=[$12], i_manufact_id_node_7=[$13], i_manufact_node_7=[$14], i_size_node_7=[$15], i_formulation_node_7=[$16], i_color_node_7=[$17], i_units_node_7=[$18], i_container_node_7=[$19], i_manager_id_node_7=[$20], i_product_name_node_7=[$21])
            +- LogicalTableScan(table=[[default_catalog, default_database, item]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS i_brand_id_node_7])
+- HashAggregate(isMerge=[false], groupBy=[i_manager_id_node_7], select=[i_manager_id_node_7, MIN(i_brand_id_node_7) AS EXPR$0])
   +- Exchange(distribution=[hash[i_manager_id_node_7]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_ext_discount_amt, i_current_price_node_7)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, H1yMV, i_item_id_node_7, i_rec_start_date_node_7, i_rec_end_date_node_7, i_item_desc_node_7, i_current_price_node_7, i_wholesale_cost_node_7, i_brand_id_node_7, i_brand_node_7, i_class_id_node_7, i_class_node_7, i_category_id_node_7, i_category_node_7, i_manufact_id_node_7, i_manufact_node_7, i_size_node_7, i_formulation_node_7, i_color_node_7, i_units_node_7, i_container_node_7, i_manager_id_node_7, i_product_name_node_7], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[<=(ss_sold_date_sk, 23)])
         :     +- SortLimit(orderBy=[ss_sales_price ASC], offset=[0], fetch=[1], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[ss_sales_price ASC], offset=[0], fetch=[1], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
         +- Calc(select=[i_item_sk AS H1yMV, i_item_id AS i_item_id_node_7, i_rec_start_date AS i_rec_start_date_node_7, i_rec_end_date AS i_rec_end_date_node_7, i_item_desc AS i_item_desc_node_7, i_current_price AS i_current_price_node_7, i_wholesale_cost AS i_wholesale_cost_node_7, i_brand_id AS i_brand_id_node_7, i_brand AS i_brand_node_7, i_class_id AS i_class_id_node_7, i_class AS i_class_node_7, i_category_id AS i_category_id_node_7, i_category AS i_category_node_7, i_manufact_id AS i_manufact_id_node_7, i_manufact AS i_manufact_node_7, i_size AS i_size_node_7, i_formulation AS i_formulation_node_7, i_color AS i_color_node_7, i_units AS i_units_node_7, i_container AS i_container_node_7, i_manager_id AS i_manager_id_node_7, i_product_name AS i_product_name_node_7], where=[<=(CHAR_LENGTH(i_size), 5)])
            +- TableSourceScan(table=[[default_catalog, default_database, item, filter=[<=(CHAR_LENGTH(i_size), 5)]]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS i_brand_id_node_7])
+- HashAggregate(isMerge=[false], groupBy=[i_manager_id_node_7], select=[i_manager_id_node_7, MIN(i_brand_id_node_7) AS EXPR$0])
   +- Exchange(distribution=[hash[i_manager_id_node_7]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_ext_discount_amt = i_current_price_node_7)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, H1yMV, i_item_id_node_7, i_rec_start_date_node_7, i_rec_end_date_node_7, i_item_desc_node_7, i_current_price_node_7, i_wholesale_cost_node_7, i_brand_id_node_7, i_brand_node_7, i_class_id_node_7, i_class_node_7, i_category_id_node_7, i_category_node_7, i_manufact_id_node_7, i_manufact_node_7, i_size_node_7, i_formulation_node_7, i_color_node_7, i_units_node_7, i_container_node_7, i_manager_id_node_7, i_product_name_node_7], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[(ss_sold_date_sk <= 23)])
         :     +- SortLimit(orderBy=[ss_sales_price ASC], offset=[0], fetch=[1], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[ss_sales_price ASC], offset=[0], fetch=[1], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
         +- Calc(select=[i_item_sk AS H1yMV, i_item_id AS i_item_id_node_7, i_rec_start_date AS i_rec_start_date_node_7, i_rec_end_date AS i_rec_end_date_node_7, i_item_desc AS i_item_desc_node_7, i_current_price AS i_current_price_node_7, i_wholesale_cost AS i_wholesale_cost_node_7, i_brand_id AS i_brand_id_node_7, i_brand AS i_brand_node_7, i_class_id AS i_class_id_node_7, i_class AS i_class_node_7, i_category_id AS i_category_id_node_7, i_category AS i_category_node_7, i_manufact_id AS i_manufact_id_node_7, i_manufact AS i_manufact_node_7, i_size AS i_size_node_7, i_formulation AS i_formulation_node_7, i_color AS i_color_node_7, i_units AS i_units_node_7, i_container AS i_container_node_7, i_manager_id AS i_manager_id_node_7, i_product_name AS i_product_name_node_7], where=[(CHAR_LENGTH(i_size) <= 5)])
            +- TableSourceScan(table=[[default_catalog, default_database, item, filter=[<=(CHAR_LENGTH(i_size), 5)]]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0