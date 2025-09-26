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
    return values.quantile(0.75)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_4 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_4") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_6 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_5 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_2 = autonode_4.order_by(col('ws_wholesale_cost_node_4'))
autonode_3 = autonode_5.join(autonode_6, col('ib_income_band_sk_node_6') == col('ca_address_sk_node_5'))
autonode_1 = autonode_2.join(autonode_3, col('ca_address_sk_node_5') == col('ws_order_number_node_4'))
sink = autonode_1.group_by(col('ws_ext_sales_price_node_4')).select(col('ib_upper_bound_node_6').max.alias('ib_upper_bound_node_6'))
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
LogicalProject(ib_upper_bound_node_6=[$1])
+- LogicalAggregate(group=[{23}], EXPR$0=[MAX($49)])
   +- LogicalJoin(condition=[=($34, $17)], joinType=[inner])
      :- LogicalSort(sort0=[$19], dir0=[ASC])
      :  +- LogicalProject(ws_sold_date_sk_node_4=[$0], ws_sold_time_sk_node_4=[$1], ws_ship_date_sk_node_4=[$2], ws_item_sk_node_4=[$3], ws_bill_customer_sk_node_4=[$4], ws_bill_cdemo_sk_node_4=[$5], ws_bill_hdemo_sk_node_4=[$6], ws_bill_addr_sk_node_4=[$7], ws_ship_customer_sk_node_4=[$8], ws_ship_cdemo_sk_node_4=[$9], ws_ship_hdemo_sk_node_4=[$10], ws_ship_addr_sk_node_4=[$11], ws_web_page_sk_node_4=[$12], ws_web_site_sk_node_4=[$13], ws_ship_mode_sk_node_4=[$14], ws_warehouse_sk_node_4=[$15], ws_promo_sk_node_4=[$16], ws_order_number_node_4=[$17], ws_quantity_node_4=[$18], ws_wholesale_cost_node_4=[$19], ws_list_price_node_4=[$20], ws_sales_price_node_4=[$21], ws_ext_discount_amt_node_4=[$22], ws_ext_sales_price_node_4=[$23], ws_ext_wholesale_cost_node_4=[$24], ws_ext_list_price_node_4=[$25], ws_ext_tax_node_4=[$26], ws_coupon_amt_node_4=[$27], ws_ext_ship_cost_node_4=[$28], ws_net_paid_node_4=[$29], ws_net_paid_inc_tax_node_4=[$30], ws_net_paid_inc_ship_node_4=[$31], ws_net_paid_inc_ship_tax_node_4=[$32], ws_net_profit_node_4=[$33])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
      +- LogicalJoin(condition=[=($13, $0)], joinType=[inner])
         :- LogicalProject(ca_address_sk_node_5=[$0], ca_address_id_node_5=[$1], ca_street_number_node_5=[$2], ca_street_name_node_5=[$3], ca_street_type_node_5=[$4], ca_suite_number_node_5=[$5], ca_city_node_5=[$6], ca_county_node_5=[$7], ca_state_node_5=[$8], ca_zip_node_5=[$9], ca_country_node_5=[$10], ca_gmt_offset_node_5=[$11], ca_location_type_node_5=[$12])
         :  +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
         +- LogicalProject(ib_income_band_sk_node_6=[$0], ib_lower_bound_node_6=[$1], ib_upper_bound_node_6=[$2])
            +- LogicalTableScan(table=[[default_catalog, default_database, income_band]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ib_upper_bound_node_6])
+- HashAggregate(isMerge=[false], groupBy=[ws_ext_sales_price], select=[ws_ext_sales_price, MAX(ib_upper_bound) AS EXPR$0])
   +- Exchange(distribution=[hash[ws_ext_sales_price]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ca_address_sk, ws_order_number)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, ib_income_band_sk, ib_lower_bound, ib_upper_bound], build=[right])
         :- Sort(orderBy=[ws_wholesale_cost ASC])
         :  +- Exchange(distribution=[single])
         :     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
         +- Exchange(distribution=[broadcast])
            +- HashJoin(joinType=[InnerJoin], where=[=(ib_income_band_sk, ca_address_sk)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, ib_income_band_sk, ib_lower_bound, ib_upper_bound], isBroadcast=[true], build=[right])
               :- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
               +- Exchange(distribution=[broadcast])
                  +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ib_upper_bound_node_6])
+- HashAggregate(isMerge=[false], groupBy=[ws_ext_sales_price], select=[ws_ext_sales_price, MAX(ib_upper_bound) AS EXPR$0])
   +- Exchange(distribution=[hash[ws_ext_sales_price]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[(ca_address_sk = ws_order_number)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, ib_income_band_sk, ib_lower_bound, ib_upper_bound], build=[right])
         :- Sort(orderBy=[ws_wholesale_cost ASC])
         :  +- Exchange(distribution=[single])
         :     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
         +- Exchange(distribution=[broadcast])
            +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(ib_income_band_sk = ca_address_sk)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, ib_income_band_sk, ib_lower_bound, ib_upper_bound], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])\
+- [#2] Exchange(distribution=[broadcast])\
])
               :- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
               +- Exchange(distribution=[broadcast])
                  +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o164246545.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#331752816:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[19](input=RelSubset#331752814,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[19]), rel#331752813:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[19](input=RelSubset#331752812,groupBy=ws_order_number, ws_ext_sales_price,select=ws_order_number, ws_ext_sales_price)]
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