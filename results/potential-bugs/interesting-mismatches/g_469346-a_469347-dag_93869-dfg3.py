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


def preloaded_aggregation(values: pd.Series) -> int:
    return len(values)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_5 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_4 = autonode_6.order_by(col('ss_cdemo_sk_node_6'))
autonode_3 = autonode_5.filter(col('w_gmt_offset_node_5') < -44.26952004432678)
autonode_2 = autonode_3.join(autonode_4, col('ss_customer_sk_node_6') == col('w_warehouse_sk_node_5'))
autonode_1 = autonode_2.group_by(col('w_country_node_5')).select(col('w_warehouse_sk_node_5').min.alias('w_warehouse_sk_node_5'))
sink = autonode_1.group_by(col('w_warehouse_sk_node_5')).select(col('w_warehouse_sk_node_5').max.alias('w_warehouse_sk_node_5'))
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
      "error_message": "An error occurred while calling o255560759.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#516717290:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[4](input=RelSubset#516717288,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[4]), rel#516717287:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[4](input=RelSubset#516717286,groupBy=ss_customer_sk,select=ss_customer_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (4) must be less than size (1)
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
LogicalProject(w_warehouse_sk_node_5=[$1])
+- LogicalAggregate(group=[{0}], EXPR$0=[MAX($0)])
   +- LogicalProject(w_warehouse_sk_node_5=[$1])
      +- LogicalAggregate(group=[{12}], EXPR$0=[MIN($0)])
         +- LogicalJoin(condition=[=($17, $0)], joinType=[inner])
            :- LogicalFilter(condition=[<($13, -4.426952004432678E1:DOUBLE)])
            :  +- LogicalProject(w_warehouse_sk_node_5=[$0], w_warehouse_id_node_5=[$1], w_warehouse_name_node_5=[$2], w_warehouse_sq_ft_node_5=[$3], w_street_number_node_5=[$4], w_street_name_node_5=[$5], w_street_type_node_5=[$6], w_suite_number_node_5=[$7], w_city_node_5=[$8], w_county_node_5=[$9], w_state_node_5=[$10], w_zip_node_5=[$11], w_country_node_5=[$12], w_gmt_offset_node_5=[$13])
            :     +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])
            +- LogicalSort(sort0=[$4], dir0=[ASC])
               +- LogicalProject(ss_sold_date_sk_node_6=[$0], ss_sold_time_sk_node_6=[$1], ss_item_sk_node_6=[$2], ss_customer_sk_node_6=[$3], ss_cdemo_sk_node_6=[$4], ss_hdemo_sk_node_6=[$5], ss_addr_sk_node_6=[$6], ss_store_sk_node_6=[$7], ss_promo_sk_node_6=[$8], ss_ticket_number_node_6=[$9], ss_quantity_node_6=[$10], ss_wholesale_cost_node_6=[$11], ss_list_price_node_6=[$12], ss_sales_price_node_6=[$13], ss_ext_discount_amt_node_6=[$14], ss_ext_sales_price_node_6=[$15], ss_ext_wholesale_cost_node_6=[$16], ss_ext_list_price_node_6=[$17], ss_ext_tax_node_6=[$18], ss_coupon_amt_node_6=[$19], ss_net_paid_node_6=[$20], ss_net_paid_inc_tax_node_6=[$21], ss_net_profit_node_6=[$22])
                  +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS w_warehouse_sk_node_5])
+- SortAggregate(isMerge=[false], groupBy=[w_warehouse_sk_node_5], select=[w_warehouse_sk_node_5, MAX(w_warehouse_sk_node_5) AS EXPR$0])
   +- Sort(orderBy=[w_warehouse_sk_node_5 ASC])
      +- Exchange(distribution=[hash[w_warehouse_sk_node_5]])
         +- Calc(select=[EXPR$0 AS w_warehouse_sk_node_5])
            +- SortAggregate(isMerge=[false], groupBy=[w_country], select=[w_country, MIN(w_warehouse_sk) AS EXPR$0])
               +- Sort(orderBy=[w_country ASC])
                  +- Exchange(distribution=[hash[w_country]])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_customer_sk, w_warehouse_sk)], select=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[right])
                        :- Calc(select=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], where=[<(w_gmt_offset, -4.426952004432678E1)])
                        :  +- TableSourceScan(table=[[default_catalog, default_database, warehouse, filter=[<(w_gmt_offset, -4.426952004432678E1:DOUBLE)]]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
                        +- Exchange(distribution=[broadcast])
                           +- SortLimit(orderBy=[ss_cdemo_sk ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[ss_cdemo_sk ASC], offset=[0], fetch=[1], global=[false])
                                    +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS w_warehouse_sk_node_5])
+- SortAggregate(isMerge=[false], groupBy=[w_warehouse_sk_node_5], select=[w_warehouse_sk_node_5, MAX(w_warehouse_sk_node_5) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[w_warehouse_sk_node_5 ASC])
         +- Exchange(distribution=[hash[w_warehouse_sk_node_5]])
            +- Calc(select=[EXPR$0 AS w_warehouse_sk_node_5])
               +- SortAggregate(isMerge=[false], groupBy=[w_country], select=[w_country, MIN(w_warehouse_sk) AS EXPR$0])
                  +- Exchange(distribution=[forward])
                     +- Sort(orderBy=[w_country ASC])
                        +- Exchange(distribution=[hash[w_country]])
                           +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_customer_sk = w_warehouse_sk)], select=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[right])
                              :- Calc(select=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], where=[(w_gmt_offset < -4.426952004432678E1)])
                              :  +- TableSourceScan(table=[[default_catalog, default_database, warehouse, filter=[<(w_gmt_offset, -4.426952004432678E1:DOUBLE)]]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
                              +- Exchange(distribution=[broadcast])
                                 +- SortLimit(orderBy=[ss_cdemo_sk ASC], offset=[0], fetch=[1], global=[true])
                                    +- Exchange(distribution=[single])
                                       +- SortLimit(orderBy=[ss_cdemo_sk ASC], offset=[0], fetch=[1], global=[false])
                                          +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0