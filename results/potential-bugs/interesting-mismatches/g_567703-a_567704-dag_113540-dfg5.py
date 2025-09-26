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

autonode_6 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_7 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_4 = autonode_6.alias('Dc9j3')
autonode_5 = autonode_7.order_by(col('ss_store_sk_node_7'))
autonode_3 = autonode_4.join(autonode_5, col('c_first_shipto_date_sk_node_6') == col('ss_customer_sk_node_7'))
autonode_2 = autonode_3.group_by(col('ss_promo_sk_node_7')).select(col('c_last_review_date_node_6').max.alias('c_last_review_date_node_6'))
autonode_1 = autonode_2.filter(col('c_last_review_date_node_6').char_length < 5)
sink = autonode_1.alias('lI0yA')
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
LogicalProject(lI0yA=[AS($0, _UTF-16LE'lI0yA')])
+- LogicalFilter(condition=[<(CHAR_LENGTH($0), 5)])
   +- LogicalProject(c_last_review_date_node_6=[$1])
      +- LogicalAggregate(group=[{26}], EXPR$0=[MAX($17)])
         +- LogicalJoin(condition=[=($5, $21)], joinType=[inner])
            :- LogicalProject(Dc9j3=[AS($0, _UTF-16LE'Dc9j3')], c_customer_id_node_6=[$1], c_current_cdemo_sk_node_6=[$2], c_current_hdemo_sk_node_6=[$3], c_current_addr_sk_node_6=[$4], c_first_shipto_date_sk_node_6=[$5], c_first_sales_date_sk_node_6=[$6], c_salutation_node_6=[$7], c_first_name_node_6=[$8], c_last_name_node_6=[$9], c_preferred_cust_flag_node_6=[$10], c_birth_day_node_6=[$11], c_birth_month_node_6=[$12], c_birth_year_node_6=[$13], c_birth_country_node_6=[$14], c_login_node_6=[$15], c_email_address_node_6=[$16], c_last_review_date_node_6=[$17])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
            +- LogicalSort(sort0=[$7], dir0=[ASC])
               +- LogicalProject(ss_sold_date_sk_node_7=[$0], ss_sold_time_sk_node_7=[$1], ss_item_sk_node_7=[$2], ss_customer_sk_node_7=[$3], ss_cdemo_sk_node_7=[$4], ss_hdemo_sk_node_7=[$5], ss_addr_sk_node_7=[$6], ss_store_sk_node_7=[$7], ss_promo_sk_node_7=[$8], ss_ticket_number_node_7=[$9], ss_quantity_node_7=[$10], ss_wholesale_cost_node_7=[$11], ss_list_price_node_7=[$12], ss_sales_price_node_7=[$13], ss_ext_discount_amt_node_7=[$14], ss_ext_sales_price_node_7=[$15], ss_ext_wholesale_cost_node_7=[$16], ss_ext_list_price_node_7=[$17], ss_ext_tax_node_7=[$18], ss_coupon_amt_node_7=[$19], ss_net_paid_node_7=[$20], ss_net_paid_inc_tax_node_7=[$21], ss_net_profit_node_7=[$22])
                  +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS lI0yA], where=[<(CHAR_LENGTH(EXPR$0), 5)])
+- SortAggregate(isMerge=[false], groupBy=[ss_promo_sk], select=[ss_promo_sk, MAX(c_last_review_date_node_6) AS EXPR$0])
   +- Sort(orderBy=[ss_promo_sk ASC])
      +- Exchange(distribution=[hash[ss_promo_sk]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(c_first_shipto_date_sk_node_6, ss_customer_sk)], select=[Dc9j3, c_customer_id_node_6, c_current_cdemo_sk_node_6, c_current_hdemo_sk_node_6, c_current_addr_sk_node_6, c_first_shipto_date_sk_node_6, c_first_sales_date_sk_node_6, c_salutation_node_6, c_first_name_node_6, c_last_name_node_6, c_preferred_cust_flag_node_6, c_birth_day_node_6, c_birth_month_node_6, c_birth_year_node_6, c_birth_country_node_6, c_login_node_6, c_email_address_node_6, c_last_review_date_node_6, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[c_customer_sk AS Dc9j3, c_customer_id AS c_customer_id_node_6, c_current_cdemo_sk AS c_current_cdemo_sk_node_6, c_current_hdemo_sk AS c_current_hdemo_sk_node_6, c_current_addr_sk AS c_current_addr_sk_node_6, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_6, c_first_sales_date_sk AS c_first_sales_date_sk_node_6, c_salutation AS c_salutation_node_6, c_first_name AS c_first_name_node_6, c_last_name AS c_last_name_node_6, c_preferred_cust_flag AS c_preferred_cust_flag_node_6, c_birth_day AS c_birth_day_node_6, c_birth_month AS c_birth_month_node_6, c_birth_year AS c_birth_year_node_6, c_birth_country AS c_birth_country_node_6, c_login AS c_login_node_6, c_email_address AS c_email_address_node_6, c_last_review_date AS c_last_review_date_node_6])
            :     +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
            +- Sort(orderBy=[ss_store_sk ASC])
               +- Exchange(distribution=[single])
                  +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS lI0yA], where=[(CHAR_LENGTH(EXPR$0) < 5)])
+- SortAggregate(isMerge=[false], groupBy=[ss_promo_sk], select=[ss_promo_sk, MAX(c_last_review_date_node_6) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[ss_promo_sk ASC])
         +- Exchange(distribution=[hash[ss_promo_sk]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(c_first_shipto_date_sk_node_6 = ss_customer_sk)], select=[Dc9j3, c_customer_id_node_6, c_current_cdemo_sk_node_6, c_current_hdemo_sk_node_6, c_current_addr_sk_node_6, c_first_shipto_date_sk_node_6, c_first_sales_date_sk_node_6, c_salutation_node_6, c_first_name_node_6, c_last_name_node_6, c_preferred_cust_flag_node_6, c_birth_day_node_6, c_birth_month_node_6, c_birth_year_node_6, c_birth_country_node_6, c_login_node_6, c_email_address_node_6, c_last_review_date_node_6, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[c_customer_sk AS Dc9j3, c_customer_id AS c_customer_id_node_6, c_current_cdemo_sk AS c_current_cdemo_sk_node_6, c_current_hdemo_sk AS c_current_hdemo_sk_node_6, c_current_addr_sk AS c_current_addr_sk_node_6, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_6, c_first_sales_date_sk AS c_first_sales_date_sk_node_6, c_salutation AS c_salutation_node_6, c_first_name AS c_first_name_node_6, c_last_name AS c_last_name_node_6, c_preferred_cust_flag AS c_preferred_cust_flag_node_6, c_birth_day AS c_birth_day_node_6, c_birth_month AS c_birth_month_node_6, c_birth_year AS c_birth_year_node_6, c_birth_country AS c_birth_country_node_6, c_login AS c_login_node_6, c_email_address AS c_email_address_node_6, c_last_review_date AS c_last_review_date_node_6])
               :     +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
               +- Sort(orderBy=[ss_store_sk ASC])
                  +- Exchange(distribution=[single])
                     +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o309577619.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#624142751:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[7](input=RelSubset#624142749,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[7]), rel#624142748:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[7](input=RelSubset#624142747,groupBy=ss_customer_sk, ss_promo_sk,select=ss_customer_sk, ss_promo_sk)]
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