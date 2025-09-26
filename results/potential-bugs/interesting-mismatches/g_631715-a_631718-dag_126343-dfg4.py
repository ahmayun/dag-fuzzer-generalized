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
    return values.sum()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_8 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_7 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_6 = autonode_8.order_by(col('cs_ext_ship_cost_node_8'))
autonode_5 = autonode_7.alias('ye7AM')
autonode_4 = autonode_5.join(autonode_6, col('wp_char_count_node_7') == col('cs_bill_hdemo_sk_node_8'))
autonode_3 = autonode_4.group_by(col('cs_ext_discount_amt_node_8')).select(col('cs_net_paid_node_8').min.alias('cs_net_paid_node_8'))
autonode_2 = autonode_3.limit(91)
autonode_1 = autonode_2.add_columns(lit("hello"))
sink = autonode_1.distinct()
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
LogicalAggregate(group=[{0, 1}])
+- LogicalProject(cs_net_paid_node_8=[$0], _c1=[_UTF-16LE'hello'])
   +- LogicalSort(fetch=[91])
      +- LogicalProject(cs_net_paid_node_8=[$1])
         +- LogicalAggregate(group=[{36}], EXPR$0=[MIN($43)])
            +- LogicalJoin(condition=[=($10, $19)], joinType=[inner])
               :- LogicalProject(ye7AM=[AS($0, _UTF-16LE'ye7AM')], wp_web_page_id_node_7=[$1], wp_rec_start_date_node_7=[$2], wp_rec_end_date_node_7=[$3], wp_creation_date_sk_node_7=[$4], wp_access_date_sk_node_7=[$5], wp_autogen_flag_node_7=[$6], wp_customer_sk_node_7=[$7], wp_url_node_7=[$8], wp_type_node_7=[$9], wp_char_count_node_7=[$10], wp_link_count_node_7=[$11], wp_image_count_node_7=[$12], wp_max_ad_count_node_7=[$13])
               :  +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
               +- LogicalSort(sort0=[$28], dir0=[ASC])
                  +- LogicalProject(cs_sold_date_sk_node_8=[$0], cs_sold_time_sk_node_8=[$1], cs_ship_date_sk_node_8=[$2], cs_bill_customer_sk_node_8=[$3], cs_bill_cdemo_sk_node_8=[$4], cs_bill_hdemo_sk_node_8=[$5], cs_bill_addr_sk_node_8=[$6], cs_ship_customer_sk_node_8=[$7], cs_ship_cdemo_sk_node_8=[$8], cs_ship_hdemo_sk_node_8=[$9], cs_ship_addr_sk_node_8=[$10], cs_call_center_sk_node_8=[$11], cs_catalog_page_sk_node_8=[$12], cs_ship_mode_sk_node_8=[$13], cs_warehouse_sk_node_8=[$14], cs_item_sk_node_8=[$15], cs_promo_sk_node_8=[$16], cs_order_number_node_8=[$17], cs_quantity_node_8=[$18], cs_wholesale_cost_node_8=[$19], cs_list_price_node_8=[$20], cs_sales_price_node_8=[$21], cs_ext_discount_amt_node_8=[$22], cs_ext_sales_price_node_8=[$23], cs_ext_wholesale_cost_node_8=[$24], cs_ext_list_price_node_8=[$25], cs_ext_tax_node_8=[$26], cs_coupon_amt_node_8=[$27], cs_ext_ship_cost_node_8=[$28], cs_net_paid_node_8=[$29], cs_net_paid_inc_tax_node_8=[$30], cs_net_paid_inc_ship_node_8=[$31], cs_net_paid_inc_ship_tax_node_8=[$32], cs_net_profit_node_8=[$33])
                     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])

== Optimized Physical Plan ==
Calc(select=[cs_net_paid_node_8, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[cs_net_paid_node_8], select=[cs_net_paid_node_8])
   +- Exchange(distribution=[hash[cs_net_paid_node_8]])
      +- LocalHashAggregate(groupBy=[cs_net_paid_node_8], select=[cs_net_paid_node_8])
         +- Calc(select=[EXPR$0 AS cs_net_paid_node_8])
            +- Limit(offset=[0], fetch=[91], global=[true])
               +- Exchange(distribution=[single])
                  +- Limit(offset=[0], fetch=[91], global=[false])
                     +- HashAggregate(isMerge=[false], groupBy=[cs_ext_discount_amt], select=[cs_ext_discount_amt, MIN(cs_net_paid) AS EXPR$0])
                        +- Exchange(distribution=[hash[cs_ext_discount_amt]])
                           +- HashJoin(joinType=[InnerJoin], where=[=(wp_char_count_node_7, cs_bill_hdemo_sk)], select=[ye7AM, wp_web_page_id_node_7, wp_rec_start_date_node_7, wp_rec_end_date_node_7, wp_creation_date_sk_node_7, wp_access_date_sk_node_7, wp_autogen_flag_node_7, wp_customer_sk_node_7, wp_url_node_7, wp_type_node_7, wp_char_count_node_7, wp_link_count_node_7, wp_image_count_node_7, wp_max_ad_count_node_7, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], isBroadcast=[true], build=[left])
                              :- Exchange(distribution=[broadcast])
                              :  +- Calc(select=[wp_web_page_sk AS ye7AM, wp_web_page_id AS wp_web_page_id_node_7, wp_rec_start_date AS wp_rec_start_date_node_7, wp_rec_end_date AS wp_rec_end_date_node_7, wp_creation_date_sk AS wp_creation_date_sk_node_7, wp_access_date_sk AS wp_access_date_sk_node_7, wp_autogen_flag AS wp_autogen_flag_node_7, wp_customer_sk AS wp_customer_sk_node_7, wp_url AS wp_url_node_7, wp_type AS wp_type_node_7, wp_char_count AS wp_char_count_node_7, wp_link_count AS wp_link_count_node_7, wp_image_count AS wp_image_count_node_7, wp_max_ad_count AS wp_max_ad_count_node_7])
                              :     +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                              +- Sort(orderBy=[cs_ext_ship_cost ASC])
                                 +- Exchange(distribution=[single])
                                    +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

== Optimized Execution Plan ==
Calc(select=[cs_net_paid_node_8, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[cs_net_paid_node_8], select=[cs_net_paid_node_8])
   +- Exchange(distribution=[hash[cs_net_paid_node_8]])
      +- LocalHashAggregate(groupBy=[cs_net_paid_node_8], select=[cs_net_paid_node_8])
         +- Calc(select=[EXPR$0 AS cs_net_paid_node_8])
            +- Limit(offset=[0], fetch=[91], global=[true])
               +- Exchange(distribution=[single])
                  +- Limit(offset=[0], fetch=[91], global=[false])
                     +- HashAggregate(isMerge=[false], groupBy=[cs_ext_discount_amt], select=[cs_ext_discount_amt, MIN(cs_net_paid) AS EXPR$0])
                        +- Exchange(distribution=[hash[cs_ext_discount_amt]])
                           +- HashJoin(joinType=[InnerJoin], where=[(wp_char_count_node_7 = cs_bill_hdemo_sk)], select=[ye7AM, wp_web_page_id_node_7, wp_rec_start_date_node_7, wp_rec_end_date_node_7, wp_creation_date_sk_node_7, wp_access_date_sk_node_7, wp_autogen_flag_node_7, wp_customer_sk_node_7, wp_url_node_7, wp_type_node_7, wp_char_count_node_7, wp_link_count_node_7, wp_image_count_node_7, wp_max_ad_count_node_7, cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit], isBroadcast=[true], build=[left])
                              :- Exchange(distribution=[broadcast])
                              :  +- Calc(select=[wp_web_page_sk AS ye7AM, wp_web_page_id AS wp_web_page_id_node_7, wp_rec_start_date AS wp_rec_start_date_node_7, wp_rec_end_date AS wp_rec_end_date_node_7, wp_creation_date_sk AS wp_creation_date_sk_node_7, wp_access_date_sk AS wp_access_date_sk_node_7, wp_autogen_flag AS wp_autogen_flag_node_7, wp_customer_sk AS wp_customer_sk_node_7, wp_url AS wp_url_node_7, wp_type AS wp_type_node_7, wp_char_count AS wp_char_count_node_7, wp_link_count AS wp_link_count_node_7, wp_image_count AS wp_image_count_node_7, wp_max_ad_count AS wp_max_ad_count_node_7])
                              :     +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                              +- Sort(orderBy=[cs_ext_ship_cost ASC])
                                 +- Exchange(distribution=[single])
                                    +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o344489678.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#694317130:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[28](input=RelSubset#694317128,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[28]), rel#694317127:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[28](input=RelSubset#694317126,groupBy=cs_bill_hdemo_sk, cs_ext_discount_amt,select=cs_bill_hdemo_sk, cs_ext_discount_amt, Partial_MIN(cs_net_paid) AS min$0)]
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