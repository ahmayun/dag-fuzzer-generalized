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

autonode_8 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_7 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_6 = autonode_8.distinct()
autonode_5 = autonode_7.order_by(col('ss_customer_sk_node_7'))
autonode_4 = autonode_5.join(autonode_6, col('ss_store_sk_node_7') == col('cd_dep_college_count_node_8'))
autonode_3 = autonode_4.add_columns(lit("hello"))
autonode_2 = autonode_3.filter(col('cd_marital_status_node_8').char_length > 5)
autonode_1 = autonode_2.group_by(col('ss_coupon_amt_node_7')).select(col('ss_hdemo_sk_node_7').max.alias('ss_hdemo_sk_node_7'))
sink = autonode_1.group_by(col('ss_hdemo_sk_node_7')).select(col('ss_hdemo_sk_node_7').max.alias('ss_hdemo_sk_node_7'))
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
LogicalProject(ss_hdemo_sk_node_7=[$1])
+- LogicalAggregate(group=[{0}], EXPR$0=[MAX($0)])
   +- LogicalProject(ss_hdemo_sk_node_7=[$1])
      +- LogicalAggregate(group=[{19}], EXPR$0=[MAX($5)])
         +- LogicalFilter(condition=[>(CHAR_LENGTH($25), 5)])
            +- LogicalProject(ss_sold_date_sk_node_7=[$0], ss_sold_time_sk_node_7=[$1], ss_item_sk_node_7=[$2], ss_customer_sk_node_7=[$3], ss_cdemo_sk_node_7=[$4], ss_hdemo_sk_node_7=[$5], ss_addr_sk_node_7=[$6], ss_store_sk_node_7=[$7], ss_promo_sk_node_7=[$8], ss_ticket_number_node_7=[$9], ss_quantity_node_7=[$10], ss_wholesale_cost_node_7=[$11], ss_list_price_node_7=[$12], ss_sales_price_node_7=[$13], ss_ext_discount_amt_node_7=[$14], ss_ext_sales_price_node_7=[$15], ss_ext_wholesale_cost_node_7=[$16], ss_ext_list_price_node_7=[$17], ss_ext_tax_node_7=[$18], ss_coupon_amt_node_7=[$19], ss_net_paid_node_7=[$20], ss_net_paid_inc_tax_node_7=[$21], ss_net_profit_node_7=[$22], cd_demo_sk_node_8=[$23], cd_gender_node_8=[$24], cd_marital_status_node_8=[$25], cd_education_status_node_8=[$26], cd_purchase_estimate_node_8=[$27], cd_credit_rating_node_8=[$28], cd_dep_count_node_8=[$29], cd_dep_employed_count_node_8=[$30], cd_dep_college_count_node_8=[$31], _c32=[_UTF-16LE'hello'])
               +- LogicalJoin(condition=[=($7, $31)], joinType=[inner])
                  :- LogicalSort(sort0=[$3], dir0=[ASC])
                  :  +- LogicalProject(ss_sold_date_sk_node_7=[$0], ss_sold_time_sk_node_7=[$1], ss_item_sk_node_7=[$2], ss_customer_sk_node_7=[$3], ss_cdemo_sk_node_7=[$4], ss_hdemo_sk_node_7=[$5], ss_addr_sk_node_7=[$6], ss_store_sk_node_7=[$7], ss_promo_sk_node_7=[$8], ss_ticket_number_node_7=[$9], ss_quantity_node_7=[$10], ss_wholesale_cost_node_7=[$11], ss_list_price_node_7=[$12], ss_sales_price_node_7=[$13], ss_ext_discount_amt_node_7=[$14], ss_ext_sales_price_node_7=[$15], ss_ext_wholesale_cost_node_7=[$16], ss_ext_list_price_node_7=[$17], ss_ext_tax_node_7=[$18], ss_coupon_amt_node_7=[$19], ss_net_paid_node_7=[$20], ss_net_paid_inc_tax_node_7=[$21], ss_net_profit_node_7=[$22])
                  :     +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
                  +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8}])
                     +- LogicalProject(cd_demo_sk_node_8=[$0], cd_gender_node_8=[$1], cd_marital_status_node_8=[$2], cd_education_status_node_8=[$3], cd_purchase_estimate_node_8=[$4], cd_credit_rating_node_8=[$5], cd_dep_count_node_8=[$6], cd_dep_employed_count_node_8=[$7], cd_dep_college_count_node_8=[$8])
                        +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ss_hdemo_sk_node_7])
+- HashAggregate(isMerge=[true], groupBy=[ss_hdemo_sk_node_7], select=[ss_hdemo_sk_node_7, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_hdemo_sk_node_7]])
      +- LocalHashAggregate(groupBy=[ss_hdemo_sk_node_7], select=[ss_hdemo_sk_node_7, Partial_MAX(ss_hdemo_sk_node_7) AS max$0])
         +- Calc(select=[EXPR$0 AS ss_hdemo_sk_node_7])
            +- HashAggregate(isMerge=[true], groupBy=[ss_coupon_amt], select=[ss_coupon_amt, Final_MAX(max$0) AS EXPR$0])
               +- Exchange(distribution=[hash[ss_coupon_amt]])
                  +- LocalHashAggregate(groupBy=[ss_coupon_amt], select=[ss_coupon_amt, Partial_MAX(ss_hdemo_sk) AS max$0])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_store_sk, cd_dep_college_count)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[right])
                        :- Sort(orderBy=[ss_customer_sk ASC])
                        :  +- Exchange(distribution=[single])
                        :     +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                        +- Exchange(distribution=[broadcast])
                           +- HashAggregate(isMerge=[true], groupBy=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                              +- Exchange(distribution=[hash[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count]])
                                 +- LocalHashAggregate(groupBy=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                                    +- Calc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[>(CHAR_LENGTH(cd_marital_status), 5)])
                                       +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[>(CHAR_LENGTH(cd_marital_status), 5)]]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ss_hdemo_sk_node_7])
+- HashAggregate(isMerge=[true], groupBy=[ss_hdemo_sk_node_7], select=[ss_hdemo_sk_node_7, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_hdemo_sk_node_7]])
      +- LocalHashAggregate(groupBy=[ss_hdemo_sk_node_7], select=[ss_hdemo_sk_node_7, Partial_MAX(ss_hdemo_sk_node_7) AS max$0])
         +- Calc(select=[EXPR$0 AS ss_hdemo_sk_node_7])
            +- HashAggregate(isMerge=[true], groupBy=[ss_coupon_amt], select=[ss_coupon_amt, Final_MAX(max$0) AS EXPR$0])
               +- Exchange(distribution=[hash[ss_coupon_amt]])
                  +- LocalHashAggregate(groupBy=[ss_coupon_amt], select=[ss_coupon_amt, Partial_MAX(ss_hdemo_sk) AS max$0])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_store_sk = cd_dep_college_count)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[right])
                        :- Sort(orderBy=[ss_customer_sk ASC])
                        :  +- Exchange(distribution=[single])
                        :     +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                        +- Exchange(distribution=[broadcast])
                           +- HashAggregate(isMerge=[true], groupBy=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                              +- Exchange(distribution=[hash[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count]])
                                 +- LocalHashAggregate(groupBy=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                                    +- Calc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[(CHAR_LENGTH(cd_marital_status) > 5)])
                                       +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[>(CHAR_LENGTH(cd_marital_status), 5)]]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o300607997.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#606474715:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[3](input=RelSubset#606474713,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[3]), rel#606474712:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#606474711,groupBy=ss_store_sk, ss_coupon_amt,select=ss_store_sk, ss_coupon_amt, Partial_MAX(ss_hdemo_sk) AS max$0)]
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