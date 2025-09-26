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

autonode_4 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_4") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_5 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_2 = autonode_4.limit(65)
autonode_3 = autonode_5.order_by(col('ca_street_number_node_5'))
autonode_1 = autonode_2.join(autonode_3, col('ca_gmt_offset_node_5') == col('ss_net_profit_node_4'))
sink = autonode_1.group_by(col('ss_ext_list_price_node_4')).select(col('ss_net_profit_node_4').min.alias('ss_net_profit_node_4'))
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
      "error_message": "An error occurred while calling o293669504.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#592702588:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[2](input=RelSubset#592702586,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[2]), rel#592702585:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[2](input=RelSubset#592702584,groupBy=ca_gmt_offset_node_50,select=ca_gmt_offset_node_50)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (2) must be less than size (1)
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
LogicalProject(ss_net_profit_node_4=[$1])
+- LogicalAggregate(group=[{17}], EXPR$0=[MIN($22)])
   +- LogicalJoin(condition=[=($34, $22)], joinType=[inner])
      :- LogicalSort(fetch=[65])
      :  +- LogicalProject(ss_sold_date_sk_node_4=[$0], ss_sold_time_sk_node_4=[$1], ss_item_sk_node_4=[$2], ss_customer_sk_node_4=[$3], ss_cdemo_sk_node_4=[$4], ss_hdemo_sk_node_4=[$5], ss_addr_sk_node_4=[$6], ss_store_sk_node_4=[$7], ss_promo_sk_node_4=[$8], ss_ticket_number_node_4=[$9], ss_quantity_node_4=[$10], ss_wholesale_cost_node_4=[$11], ss_list_price_node_4=[$12], ss_sales_price_node_4=[$13], ss_ext_discount_amt_node_4=[$14], ss_ext_sales_price_node_4=[$15], ss_ext_wholesale_cost_node_4=[$16], ss_ext_list_price_node_4=[$17], ss_ext_tax_node_4=[$18], ss_coupon_amt_node_4=[$19], ss_net_paid_node_4=[$20], ss_net_paid_inc_tax_node_4=[$21], ss_net_profit_node_4=[$22])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalSort(sort0=[$2], dir0=[ASC])
         +- LogicalProject(ca_address_sk_node_5=[$0], ca_address_id_node_5=[$1], ca_street_number_node_5=[$2], ca_street_name_node_5=[$3], ca_street_type_node_5=[$4], ca_suite_number_node_5=[$5], ca_city_node_5=[$6], ca_county_node_5=[$7], ca_state_node_5=[$8], ca_zip_node_5=[$9], ca_country_node_5=[$10], ca_gmt_offset_node_5=[$11], ca_location_type_node_5=[$12])
            +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ss_net_profit_node_4])
+- HashAggregate(isMerge=[true], groupBy=[ss_ext_list_price_node_4], select=[ss_ext_list_price_node_4, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_ext_list_price_node_4]])
      +- LocalHashAggregate(groupBy=[ss_ext_list_price_node_4], select=[ss_ext_list_price_node_4, Partial_MIN(ss_net_profit_node_4) AS min$0])
         +- Calc(select=[ss_sold_date_sk AS ss_sold_date_sk_node_4, ss_sold_time_sk AS ss_sold_time_sk_node_4, ss_item_sk AS ss_item_sk_node_4, ss_customer_sk AS ss_customer_sk_node_4, ss_cdemo_sk AS ss_cdemo_sk_node_4, ss_hdemo_sk AS ss_hdemo_sk_node_4, ss_addr_sk AS ss_addr_sk_node_4, ss_store_sk AS ss_store_sk_node_4, ss_promo_sk AS ss_promo_sk_node_4, ss_ticket_number AS ss_ticket_number_node_4, ss_quantity AS ss_quantity_node_4, ss_wholesale_cost AS ss_wholesale_cost_node_4, ss_list_price AS ss_list_price_node_4, ss_sales_price AS ss_sales_price_node_4, ss_ext_discount_amt AS ss_ext_discount_amt_node_4, ss_ext_sales_price AS ss_ext_sales_price_node_4, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_4, ss_ext_list_price AS ss_ext_list_price_node_4, ss_ext_tax AS ss_ext_tax_node_4, ss_coupon_amt AS ss_coupon_amt_node_4, ss_net_paid AS ss_net_paid_node_4, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_4, ss_net_profit AS ss_net_profit_node_4, ca_address_sk_node_5, ca_address_id_node_5, ca_street_number_node_5, ca_street_name_node_5, ca_street_type_node_5, ca_suite_number_node_5, ca_city_node_5, ca_county_node_5, ca_state_node_5, ca_zip_node_5, ca_country_node_5, ca_gmt_offset_node_5, ca_location_type_node_5])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ca_gmt_offset_node_50, ss_net_profit)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, ca_address_sk_node_5, ca_address_id_node_5, ca_street_number_node_5, ca_street_name_node_5, ca_street_type_node_5, ca_suite_number_node_5, ca_city_node_5, ca_county_node_5, ca_state_node_5, ca_zip_node_5, ca_country_node_5, ca_gmt_offset_node_5, ca_location_type_node_5, ca_gmt_offset_node_50], build=[right])
               :- Limit(offset=[0], fetch=[65], global=[true])
               :  +- Exchange(distribution=[single])
               :     +- Limit(offset=[0], fetch=[65], global=[false])
               :        +- TableSourceScan(table=[[default_catalog, default_database, store_sales, limit=[65]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
               +- Exchange(distribution=[broadcast])
                  +- Calc(select=[ca_address_sk AS ca_address_sk_node_5, ca_address_id AS ca_address_id_node_5, ca_street_number AS ca_street_number_node_5, ca_street_name AS ca_street_name_node_5, ca_street_type AS ca_street_type_node_5, ca_suite_number AS ca_suite_number_node_5, ca_city AS ca_city_node_5, ca_county AS ca_county_node_5, ca_state AS ca_state_node_5, ca_zip AS ca_zip_node_5, ca_country AS ca_country_node_5, ca_gmt_offset AS ca_gmt_offset_node_5, ca_location_type AS ca_location_type_node_5, CAST(ca_gmt_offset AS DECIMAL(7, 2)) AS ca_gmt_offset_node_50])
                     +- SortLimit(orderBy=[ca_street_number ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[ca_street_number ASC], offset=[0], fetch=[1], global=[false])
                              +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ss_net_profit_node_4])
+- HashAggregate(isMerge=[true], groupBy=[ss_ext_list_price_node_4], select=[ss_ext_list_price_node_4, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_ext_list_price_node_4]])
      +- LocalHashAggregate(groupBy=[ss_ext_list_price_node_4], select=[ss_ext_list_price_node_4, Partial_MIN(ss_net_profit_node_4) AS min$0])
         +- Calc(select=[ss_sold_date_sk AS ss_sold_date_sk_node_4, ss_sold_time_sk AS ss_sold_time_sk_node_4, ss_item_sk AS ss_item_sk_node_4, ss_customer_sk AS ss_customer_sk_node_4, ss_cdemo_sk AS ss_cdemo_sk_node_4, ss_hdemo_sk AS ss_hdemo_sk_node_4, ss_addr_sk AS ss_addr_sk_node_4, ss_store_sk AS ss_store_sk_node_4, ss_promo_sk AS ss_promo_sk_node_4, ss_ticket_number AS ss_ticket_number_node_4, ss_quantity AS ss_quantity_node_4, ss_wholesale_cost AS ss_wholesale_cost_node_4, ss_list_price AS ss_list_price_node_4, ss_sales_price AS ss_sales_price_node_4, ss_ext_discount_amt AS ss_ext_discount_amt_node_4, ss_ext_sales_price AS ss_ext_sales_price_node_4, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_4, ss_ext_list_price AS ss_ext_list_price_node_4, ss_ext_tax AS ss_ext_tax_node_4, ss_coupon_amt AS ss_coupon_amt_node_4, ss_net_paid AS ss_net_paid_node_4, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_4, ss_net_profit AS ss_net_profit_node_4, ca_address_sk_node_5, ca_address_id_node_5, ca_street_number_node_5, ca_street_name_node_5, ca_street_type_node_5, ca_suite_number_node_5, ca_city_node_5, ca_county_node_5, ca_state_node_5, ca_zip_node_5, ca_country_node_5, ca_gmt_offset_node_5, ca_location_type_node_5])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(ca_gmt_offset_node_50 = ss_net_profit)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, ca_address_sk_node_5, ca_address_id_node_5, ca_street_number_node_5, ca_street_name_node_5, ca_street_type_node_5, ca_suite_number_node_5, ca_city_node_5, ca_county_node_5, ca_state_node_5, ca_zip_node_5, ca_country_node_5, ca_gmt_offset_node_5, ca_location_type_node_5, ca_gmt_offset_node_50], build=[right])
               :- Limit(offset=[0], fetch=[65], global=[true])
               :  +- Exchange(distribution=[single])
               :     +- Limit(offset=[0], fetch=[65], global=[false])
               :        +- TableSourceScan(table=[[default_catalog, default_database, store_sales, limit=[65]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
               +- Exchange(distribution=[broadcast])
                  +- Calc(select=[ca_address_sk AS ca_address_sk_node_5, ca_address_id AS ca_address_id_node_5, ca_street_number AS ca_street_number_node_5, ca_street_name AS ca_street_name_node_5, ca_street_type AS ca_street_type_node_5, ca_suite_number AS ca_suite_number_node_5, ca_city AS ca_city_node_5, ca_county AS ca_county_node_5, ca_state AS ca_state_node_5, ca_zip AS ca_zip_node_5, ca_country AS ca_country_node_5, ca_gmt_offset AS ca_gmt_offset_node_5, ca_location_type AS ca_location_type_node_5, CAST(ca_gmt_offset AS DECIMAL(7, 2)) AS ca_gmt_offset_node_50])
                     +- SortLimit(orderBy=[ca_street_number ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[ca_street_number ASC], offset=[0], fetch=[1], global=[false])
                              +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0