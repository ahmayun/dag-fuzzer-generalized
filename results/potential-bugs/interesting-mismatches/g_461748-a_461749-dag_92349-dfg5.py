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
    return len(values) / (1.0 / values).sum() if (values != 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_9 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_8 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_7 = autonode_9.order_by(col('ss_ext_tax_node_9'))
autonode_6 = autonode_8.order_by(col('s_suite_number_node_8'))
autonode_5 = autonode_6.join(autonode_7, col('ss_hdemo_sk_node_9') == col('s_closed_date_sk_node_8'))
autonode_4 = autonode_5.filter(col('ss_sales_price_node_9') > -28.677374124526978)
autonode_3 = autonode_4.group_by(col('s_country_node_8')).select(col('s_store_name_node_8').min.alias('s_store_name_node_8'))
autonode_2 = autonode_3.limit(79)
autonode_1 = autonode_2.order_by(col('s_store_name_node_8'))
sink = autonode_1.select(col('s_store_name_node_8'))
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
      "error_message": "An error occurred while calling o251424453.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#508354605:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[18](input=RelSubset#508354603,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[18]), rel#508354602:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[18](input=RelSubset#508354601,groupBy=ss_hdemo_sk,select=ss_hdemo_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (18) must be less than size (1)
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
LogicalProject(s_store_name_node_8=[$0])
+- LogicalSort(sort0=[$0], dir0=[ASC])
   +- LogicalSort(fetch=[79])
      +- LogicalProject(s_store_name_node_8=[$1])
         +- LogicalAggregate(group=[{26}], EXPR$0=[MIN($5)])
            +- LogicalFilter(condition=[>($42, -2.8677374124526978E1:DOUBLE)])
               +- LogicalJoin(condition=[=($34, $4)], joinType=[inner])
                  :- LogicalSort(sort0=[$21], dir0=[ASC])
                  :  +- LogicalProject(s_store_sk_node_8=[$0], s_store_id_node_8=[$1], s_rec_start_date_node_8=[$2], s_rec_end_date_node_8=[$3], s_closed_date_sk_node_8=[$4], s_store_name_node_8=[$5], s_number_employees_node_8=[$6], s_floor_space_node_8=[$7], s_hours_node_8=[$8], s_manager_node_8=[$9], s_market_id_node_8=[$10], s_geography_class_node_8=[$11], s_market_desc_node_8=[$12], s_market_manager_node_8=[$13], s_division_id_node_8=[$14], s_division_name_node_8=[$15], s_company_id_node_8=[$16], s_company_name_node_8=[$17], s_street_number_node_8=[$18], s_street_name_node_8=[$19], s_street_type_node_8=[$20], s_suite_number_node_8=[$21], s_city_node_8=[$22], s_county_node_8=[$23], s_state_node_8=[$24], s_zip_node_8=[$25], s_country_node_8=[$26], s_gmt_offset_node_8=[$27], s_tax_precentage_node_8=[$28])
                  :     +- LogicalTableScan(table=[[default_catalog, default_database, store]])
                  +- LogicalSort(sort0=[$18], dir0=[ASC])
                     +- LogicalProject(ss_sold_date_sk_node_9=[$0], ss_sold_time_sk_node_9=[$1], ss_item_sk_node_9=[$2], ss_customer_sk_node_9=[$3], ss_cdemo_sk_node_9=[$4], ss_hdemo_sk_node_9=[$5], ss_addr_sk_node_9=[$6], ss_store_sk_node_9=[$7], ss_promo_sk_node_9=[$8], ss_ticket_number_node_9=[$9], ss_quantity_node_9=[$10], ss_wholesale_cost_node_9=[$11], ss_list_price_node_9=[$12], ss_sales_price_node_9=[$13], ss_ext_discount_amt_node_9=[$14], ss_ext_sales_price_node_9=[$15], ss_ext_wholesale_cost_node_9=[$16], ss_ext_list_price_node_9=[$17], ss_ext_tax_node_9=[$18], ss_coupon_amt_node_9=[$19], ss_net_paid_node_9=[$20], ss_net_paid_inc_tax_node_9=[$21], ss_net_profit_node_9=[$22])
                        +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
SortLimit(orderBy=[s_store_name_node_8 ASC], offset=[0], fetch=[1], global=[true])
+- Exchange(distribution=[single])
   +- SortLimit(orderBy=[s_store_name_node_8 ASC], offset=[0], fetch=[1], global=[false])
      +- Limit(offset=[0], fetch=[79], global=[true])
         +- Exchange(distribution=[single])
            +- Limit(offset=[0], fetch=[79], global=[false])
               +- Calc(select=[EXPR$0 AS s_store_name_node_8])
                  +- SortAggregate(isMerge=[false], groupBy=[s_country], select=[s_country, MIN(s_store_name) AS EXPR$0])
                     +- Sort(orderBy=[s_country ASC])
                        +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_hdemo_sk, s_closed_date_sk)], select=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[right])
                           :- Exchange(distribution=[hash[s_country]])
                           :  +- SortLimit(orderBy=[s_suite_number ASC], offset=[0], fetch=[1], global=[true])
                           :     +- Exchange(distribution=[single])
                           :        +- SortLimit(orderBy=[s_suite_number ASC], offset=[0], fetch=[1], global=[false])
                           :           +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                           +- Exchange(distribution=[broadcast])
                              +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[>(ss_sales_price, -2.8677374124526978E1)])
                                 +- SortLimit(orderBy=[ss_ext_tax ASC], offset=[0], fetch=[1], global=[true])
                                    +- Exchange(distribution=[single])
                                       +- SortLimit(orderBy=[ss_ext_tax ASC], offset=[0], fetch=[1], global=[false])
                                          +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

== Optimized Execution Plan ==
SortLimit(orderBy=[s_store_name_node_8 ASC], offset=[0], fetch=[1], global=[true])
+- Exchange(distribution=[single])
   +- SortLimit(orderBy=[s_store_name_node_8 ASC], offset=[0], fetch=[1], global=[false])
      +- Limit(offset=[0], fetch=[79], global=[true])
         +- Exchange(distribution=[single])
            +- Limit(offset=[0], fetch=[79], global=[false])
               +- Calc(select=[EXPR$0 AS s_store_name_node_8])
                  +- SortAggregate(isMerge=[false], groupBy=[s_country], select=[s_country, MIN(s_store_name) AS EXPR$0])
                     +- Exchange(distribution=[forward])
                        +- Sort(orderBy=[s_country ASC])
                           +- Exchange(distribution=[keep_input_as_is[hash[s_country]]])
                              +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_hdemo_sk = s_closed_date_sk)], select=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[right])
                                 :- Exchange(distribution=[hash[s_country]])
                                 :  +- SortLimit(orderBy=[s_suite_number ASC], offset=[0], fetch=[1], global=[true])
                                 :     +- Exchange(distribution=[single])
                                 :        +- SortLimit(orderBy=[s_suite_number ASC], offset=[0], fetch=[1], global=[false])
                                 :           +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                                 +- Exchange(distribution=[broadcast])
                                    +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[(ss_sales_price > -2.8677374124526978E1)])
                                       +- SortLimit(orderBy=[ss_ext_tax ASC], offset=[0], fetch=[1], global=[true])
                                          +- Exchange(distribution=[single])
                                             +- SortLimit(orderBy=[ss_ext_tax ASC], offset=[0], fetch=[1], global=[false])
                                                +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0