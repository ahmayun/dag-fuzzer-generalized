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
    return values.product()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_7 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_4 = autonode_6.filter(col('ss_ext_sales_price_node_6') > 5.655902624130249)
autonode_5 = autonode_7.alias('6ZfzI')
autonode_2 = autonode_4.order_by(col('ss_hdemo_sk_node_6'))
autonode_3 = autonode_5.filter(col('cd_dep_count_node_7') > 48)
autonode_1 = autonode_2.join(autonode_3, col('ss_sold_date_sk_node_6') == col('cd_dep_college_count_node_7'))
sink = autonode_1.group_by(col('ss_sold_time_sk_node_6')).select(col('ss_ext_list_price_node_6').min.alias('ss_ext_list_price_node_6'))
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
LogicalProject(ss_ext_list_price_node_6=[$1])
+- LogicalAggregate(group=[{1}], EXPR$0=[MIN($17)])
   +- LogicalJoin(condition=[=($0, $31)], joinType=[inner])
      :- LogicalSort(sort0=[$5], dir0=[ASC])
      :  +- LogicalFilter(condition=[>($15, 5.655902624130249E0:DOUBLE)])
      :     +- LogicalProject(ss_sold_date_sk_node_6=[$0], ss_sold_time_sk_node_6=[$1], ss_item_sk_node_6=[$2], ss_customer_sk_node_6=[$3], ss_cdemo_sk_node_6=[$4], ss_hdemo_sk_node_6=[$5], ss_addr_sk_node_6=[$6], ss_store_sk_node_6=[$7], ss_promo_sk_node_6=[$8], ss_ticket_number_node_6=[$9], ss_quantity_node_6=[$10], ss_wholesale_cost_node_6=[$11], ss_list_price_node_6=[$12], ss_sales_price_node_6=[$13], ss_ext_discount_amt_node_6=[$14], ss_ext_sales_price_node_6=[$15], ss_ext_wholesale_cost_node_6=[$16], ss_ext_list_price_node_6=[$17], ss_ext_tax_node_6=[$18], ss_coupon_amt_node_6=[$19], ss_net_paid_node_6=[$20], ss_net_paid_inc_tax_node_6=[$21], ss_net_profit_node_6=[$22])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalFilter(condition=[>($6, 48)])
         +- LogicalProject(6ZfzI=[AS($0, _UTF-16LE'6ZfzI')], cd_gender_node_7=[$1], cd_marital_status_node_7=[$2], cd_education_status_node_7=[$3], cd_purchase_estimate_node_7=[$4], cd_credit_rating_node_7=[$5], cd_dep_count_node_7=[$6], cd_dep_employed_count_node_7=[$7], cd_dep_college_count_node_7=[$8])
            +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ss_ext_list_price_node_6])
+- HashAggregate(isMerge=[true], groupBy=[ss_sold_time_sk], select=[ss_sold_time_sk, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_sold_time_sk]])
      +- LocalHashAggregate(groupBy=[ss_sold_time_sk], select=[ss_sold_time_sk, Partial_MIN(ss_ext_list_price) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_sold_date_sk, cd_dep_college_count_node_7)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, 6ZfzI, cd_gender_node_7, cd_marital_status_node_7, cd_education_status_node_7, cd_purchase_estimate_node_7, cd_credit_rating_node_7, cd_dep_count_node_7, cd_dep_employed_count_node_7, cd_dep_college_count_node_7], build=[right])
            :- Sort(orderBy=[ss_hdemo_sk ASC])
            :  +- Exchange(distribution=[single])
            :     +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[>(ss_ext_sales_price, 5.655902624130249E0)])
            :        +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[>(ss_ext_sales_price, 5.655902624130249E0:DOUBLE)]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[cd_demo_sk AS 6ZfzI, cd_gender AS cd_gender_node_7, cd_marital_status AS cd_marital_status_node_7, cd_education_status AS cd_education_status_node_7, cd_purchase_estimate AS cd_purchase_estimate_node_7, cd_credit_rating AS cd_credit_rating_node_7, cd_dep_count AS cd_dep_count_node_7, cd_dep_employed_count AS cd_dep_employed_count_node_7, cd_dep_college_count AS cd_dep_college_count_node_7], where=[>(cd_dep_count, 48)])
                  +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[>(cd_dep_count, 48)]]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ss_ext_list_price_node_6])
+- HashAggregate(isMerge=[true], groupBy=[ss_sold_time_sk], select=[ss_sold_time_sk, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_sold_time_sk]])
      +- LocalHashAggregate(groupBy=[ss_sold_time_sk], select=[ss_sold_time_sk, Partial_MIN(ss_ext_list_price) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_sold_date_sk = cd_dep_college_count_node_7)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, 6ZfzI, cd_gender_node_7, cd_marital_status_node_7, cd_education_status_node_7, cd_purchase_estimate_node_7, cd_credit_rating_node_7, cd_dep_count_node_7, cd_dep_employed_count_node_7, cd_dep_college_count_node_7], build=[right])
            :- Sort(orderBy=[ss_hdemo_sk ASC])
            :  +- Exchange(distribution=[single])
            :     +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[(ss_ext_sales_price > 5.655902624130249E0)])
            :        +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[>(ss_ext_sales_price, 5.655902624130249E0:DOUBLE)]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[cd_demo_sk AS 6ZfzI, cd_gender AS cd_gender_node_7, cd_marital_status AS cd_marital_status_node_7, cd_education_status AS cd_education_status_node_7, cd_purchase_estimate AS cd_purchase_estimate_node_7, cd_credit_rating AS cd_credit_rating_node_7, cd_dep_count AS cd_dep_count_node_7, cd_dep_employed_count AS cd_dep_employed_count_node_7, cd_dep_college_count AS cd_dep_college_count_node_7], where=[(cd_dep_count > 48)])
                  +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[>(cd_dep_count, 48)]]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o174179127.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#352333523:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[5](input=RelSubset#352333521,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[5]), rel#352333520:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[5](input=RelSubset#352333519,groupBy=ss_sold_date_sk, ss_sold_time_sk,select=ss_sold_date_sk, ss_sold_time_sk, Partial_MIN(ss_ext_list_price) AS min$0)]
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