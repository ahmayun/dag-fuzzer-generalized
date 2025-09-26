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

autonode_10 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_12 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_11 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_9 = autonode_12.distinct()
autonode_8 = autonode_10.join(autonode_11, col('ss_item_sk_node_10') == col('cd_dep_employed_count_node_11'))
autonode_7 = autonode_9.limit(46)
autonode_6 = autonode_8.filter(col('ss_coupon_amt_node_10') <= 12.324070930480957)
autonode_5 = autonode_7.order_by(col('hd_dep_count_node_12'))
autonode_4 = autonode_6.limit(23)
autonode_3 = autonode_5.alias('OENsN')
autonode_2 = autonode_4.filter(col('cd_dep_college_count_node_11') < -2)
autonode_1 = autonode_2.join(autonode_3, col('hd_income_band_sk_node_12') == col('cd_dep_count_node_11'))
sink = autonode_1.group_by(col('ss_list_price_node_10')).select(col('ss_coupon_amt_node_10').min.alias('ss_coupon_amt_node_10'))
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
      "error_message": "An error occurred while calling o279800417.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#565967877:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[3](input=RelSubset#565967875,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[3]), rel#565967874:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#565967873,groupBy=hd_income_band_sk,select=hd_income_band_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (3) must be less than size (1)
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
LogicalProject(ss_coupon_amt_node_10=[$1])
+- LogicalAggregate(group=[{12}], EXPR$0=[MIN($19)])
   +- LogicalJoin(condition=[=($33, $29)], joinType=[inner])
      :- LogicalFilter(condition=[<($31, -2)])
      :  +- LogicalSort(fetch=[23])
      :     +- LogicalFilter(condition=[<=($19, 1.2324070930480957E1:DOUBLE)])
      :        +- LogicalJoin(condition=[=($2, $30)], joinType=[inner])
      :           :- LogicalProject(ss_sold_date_sk_node_10=[$0], ss_sold_time_sk_node_10=[$1], ss_item_sk_node_10=[$2], ss_customer_sk_node_10=[$3], ss_cdemo_sk_node_10=[$4], ss_hdemo_sk_node_10=[$5], ss_addr_sk_node_10=[$6], ss_store_sk_node_10=[$7], ss_promo_sk_node_10=[$8], ss_ticket_number_node_10=[$9], ss_quantity_node_10=[$10], ss_wholesale_cost_node_10=[$11], ss_list_price_node_10=[$12], ss_sales_price_node_10=[$13], ss_ext_discount_amt_node_10=[$14], ss_ext_sales_price_node_10=[$15], ss_ext_wholesale_cost_node_10=[$16], ss_ext_list_price_node_10=[$17], ss_ext_tax_node_10=[$18], ss_coupon_amt_node_10=[$19], ss_net_paid_node_10=[$20], ss_net_paid_inc_tax_node_10=[$21], ss_net_profit_node_10=[$22])
      :           :  +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      :           +- LogicalProject(cd_demo_sk_node_11=[$0], cd_gender_node_11=[$1], cd_marital_status_node_11=[$2], cd_education_status_node_11=[$3], cd_purchase_estimate_node_11=[$4], cd_credit_rating_node_11=[$5], cd_dep_count_node_11=[$6], cd_dep_employed_count_node_11=[$7], cd_dep_college_count_node_11=[$8])
      :              +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
      +- LogicalProject(OENsN=[AS($0, _UTF-16LE'OENsN')], hd_income_band_sk_node_12=[$1], hd_buy_potential_node_12=[$2], hd_dep_count_node_12=[$3], hd_vehicle_count_node_12=[$4])
         +- LogicalSort(sort0=[$3], dir0=[ASC])
            +- LogicalSort(fetch=[46])
               +- LogicalAggregate(group=[{0, 1, 2, 3, 4}])
                  +- LogicalProject(hd_demo_sk_node_12=[$0], hd_income_band_sk_node_12=[$1], hd_buy_potential_node_12=[$2], hd_dep_count_node_12=[$3], hd_vehicle_count_node_12=[$4])
                     +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ss_coupon_amt_node_10])
+- SortAggregate(isMerge=[true], groupBy=[ss_list_price], select=[ss_list_price, Final_MIN(min$0) AS EXPR$0])
   +- Sort(orderBy=[ss_list_price ASC])
      +- Exchange(distribution=[hash[ss_list_price]])
         +- LocalSortAggregate(groupBy=[ss_list_price], select=[ss_list_price, Partial_MIN(ss_coupon_amt) AS min$0])
            +- Sort(orderBy=[ss_list_price ASC])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(hd_income_band_sk_node_12, cd_dep_count)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, OENsN, hd_income_band_sk_node_12, hd_buy_potential_node_12, hd_dep_count_node_12, hd_vehicle_count_node_12], build=[right])
                  :- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[<(cd_dep_college_count, -2)])
                  :  +- Limit(offset=[0], fetch=[23], global=[true])
                  :     +- Exchange(distribution=[single])
                  :        +- Limit(offset=[0], fetch=[23], global=[false])
                  :           +- HashJoin(joinType=[InnerJoin], where=[=(ss_item_sk, cd_dep_employed_count)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[right])
                  :              :- Exchange(distribution=[hash[ss_item_sk]])
                  :              :  +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[<=(ss_coupon_amt, 1.2324070930480957E1)])
                  :              :     +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[<=(ss_coupon_amt, 1.2324070930480957E1:DOUBLE)]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                  :              +- Exchange(distribution=[hash[cd_dep_employed_count]])
                  :                 +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                  +- Exchange(distribution=[broadcast])
                     +- Calc(select=[hd_demo_sk AS OENsN, hd_income_band_sk AS hd_income_band_sk_node_12, hd_buy_potential AS hd_buy_potential_node_12, hd_dep_count AS hd_dep_count_node_12, hd_vehicle_count AS hd_vehicle_count_node_12])
                        +- SortLimit(orderBy=[hd_dep_count ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[hd_dep_count ASC], offset=[0], fetch=[1], global=[false])
                                 +- Limit(offset=[0], fetch=[46], global=[true])
                                    +- Exchange(distribution=[single])
                                       +- Limit(offset=[0], fetch=[46], global=[false])
                                          +- HashAggregate(isMerge=[true], groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                             +- Exchange(distribution=[hash[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count]])
                                                +- LocalHashAggregate(groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                                   +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ss_coupon_amt_node_10])
+- SortAggregate(isMerge=[true], groupBy=[ss_list_price], select=[ss_list_price, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[ss_list_price ASC])
         +- Exchange(distribution=[hash[ss_list_price]])
            +- LocalSortAggregate(groupBy=[ss_list_price], select=[ss_list_price, Partial_MIN(ss_coupon_amt) AS min$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[ss_list_price ASC])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(hd_income_band_sk_node_12 = cd_dep_count)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, OENsN, hd_income_band_sk_node_12, hd_buy_potential_node_12, hd_dep_count_node_12, hd_vehicle_count_node_12], build=[right])
                        :- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[(cd_dep_college_count < -2)])
                        :  +- Limit(offset=[0], fetch=[23], global=[true])
                        :     +- Exchange(distribution=[single])
                        :        +- Limit(offset=[0], fetch=[23], global=[false])
                        :           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ss_item_sk = cd_dep_employed_count)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[right])
                        :              :- Exchange(distribution=[hash[ss_item_sk]])
                        :              :  +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[(ss_coupon_amt <= 1.2324070930480957E1)])
                        :              :     +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[<=(ss_coupon_amt, 1.2324070930480957E1:DOUBLE)]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                        :              +- Exchange(distribution=[hash[cd_dep_employed_count]])
                        :                 +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[hd_demo_sk AS OENsN, hd_income_band_sk AS hd_income_band_sk_node_12, hd_buy_potential AS hd_buy_potential_node_12, hd_dep_count AS hd_dep_count_node_12, hd_vehicle_count AS hd_vehicle_count_node_12])
                              +- SortLimit(orderBy=[hd_dep_count ASC], offset=[0], fetch=[1], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- SortLimit(orderBy=[hd_dep_count ASC], offset=[0], fetch=[1], global=[false])
                                       +- Limit(offset=[0], fetch=[46], global=[true])
                                          +- Exchange(distribution=[single])
                                             +- Limit(offset=[0], fetch=[46], global=[false])
                                                +- HashAggregate(isMerge=[true], groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                                   +- Exchange(distribution=[hash[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count]])
                                                      +- LocalHashAggregate(groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                                         +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0