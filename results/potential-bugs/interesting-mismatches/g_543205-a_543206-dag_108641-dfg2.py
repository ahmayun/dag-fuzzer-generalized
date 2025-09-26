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

autonode_13 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_12 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_14 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_15 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_9 = autonode_13.order_by(col('ss_ext_wholesale_cost_node_13'))
autonode_8 = autonode_12.select(col('cr_returning_addr_sk_node_12'))
autonode_10 = autonode_14.distinct()
autonode_11 = autonode_15.alias('0ow6e')
autonode_5 = autonode_8.join(autonode_9, col('cr_returning_addr_sk_node_12') == col('ss_sold_time_sk_node_13'))
autonode_6 = autonode_10.filter(col('sm_code_node_14').char_length < 5)
autonode_7 = autonode_11.order_by(col('c_birth_country_node_15'))
autonode_3 = autonode_5.group_by(col('ss_ticket_number_node_13')).select(col('ss_promo_sk_node_13').min.alias('ss_promo_sk_node_13'))
autonode_4 = autonode_6.join(autonode_7, col('sm_ship_mode_sk_node_14') == col('c_current_cdemo_sk_node_15'))
autonode_2 = autonode_3.join(autonode_4, col('ss_promo_sk_node_13') == col('c_first_sales_date_sk_node_15'))
autonode_1 = autonode_2.alias('ll5X7')
sink = autonode_1.select(col('sm_carrier_node_14'))
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
LogicalProject(sm_carrier_node_14=[$5])
+- LogicalJoin(condition=[=($0, $13)], joinType=[inner])
   :- LogicalProject(ss_promo_sk_node_13=[$1])
   :  +- LogicalAggregate(group=[{10}], EXPR$0=[MIN($9)])
   :     +- LogicalJoin(condition=[=($0, $2)], joinType=[inner])
   :        :- LogicalProject(cr_returning_addr_sk_node_12=[$10])
   :        :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
   :        +- LogicalSort(sort0=[$16], dir0=[ASC])
   :           +- LogicalProject(ss_sold_date_sk_node_13=[$0], ss_sold_time_sk_node_13=[$1], ss_item_sk_node_13=[$2], ss_customer_sk_node_13=[$3], ss_cdemo_sk_node_13=[$4], ss_hdemo_sk_node_13=[$5], ss_addr_sk_node_13=[$6], ss_store_sk_node_13=[$7], ss_promo_sk_node_13=[$8], ss_ticket_number_node_13=[$9], ss_quantity_node_13=[$10], ss_wholesale_cost_node_13=[$11], ss_list_price_node_13=[$12], ss_sales_price_node_13=[$13], ss_ext_discount_amt_node_13=[$14], ss_ext_sales_price_node_13=[$15], ss_ext_wholesale_cost_node_13=[$16], ss_ext_list_price_node_13=[$17], ss_ext_tax_node_13=[$18], ss_coupon_amt_node_13=[$19], ss_net_paid_node_13=[$20], ss_net_paid_inc_tax_node_13=[$21], ss_net_profit_node_13=[$22])
   :              +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
   +- LogicalJoin(condition=[=($0, $8)], joinType=[inner])
      :- LogicalFilter(condition=[<(CHAR_LENGTH($3), 5)])
      :  +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5}])
      :     +- LogicalProject(sm_ship_mode_sk_node_14=[$0], sm_ship_mode_id_node_14=[$1], sm_type_node_14=[$2], sm_code_node_14=[$3], sm_carrier_node_14=[$4], sm_contract_node_14=[$5])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, ship_mode]])
      +- LogicalSort(sort0=[$14], dir0=[ASC])
         +- LogicalProject(0ow6e=[AS($0, _UTF-16LE'0ow6e')], c_customer_id_node_15=[$1], c_current_cdemo_sk_node_15=[$2], c_current_hdemo_sk_node_15=[$3], c_current_addr_sk_node_15=[$4], c_first_shipto_date_sk_node_15=[$5], c_first_sales_date_sk_node_15=[$6], c_salutation_node_15=[$7], c_first_name_node_15=[$8], c_last_name_node_15=[$9], c_preferred_cust_flag_node_15=[$10], c_birth_day_node_15=[$11], c_birth_month_node_15=[$12], c_birth_year_node_15=[$13], c_birth_country_node_15=[$14], c_login_node_15=[$15], c_email_address_node_15=[$16], c_last_review_date_node_15=[$17])
            +- LogicalTableScan(table=[[default_catalog, default_database, customer]])

== Optimized Physical Plan ==
Calc(select=[sm_carrier_node_14])
+- HashJoin(joinType=[InnerJoin], where=[=(ss_promo_sk_node_13, c_first_sales_date_sk_node_15)], select=[ss_promo_sk_node_13, sm_carrier_node_14, c_first_sales_date_sk_node_15], isBroadcast=[true], build=[right])
   :- Calc(select=[EXPR$0 AS ss_promo_sk_node_13])
   :  +- HashAggregate(isMerge=[true], groupBy=[ss_ticket_number], select=[ss_ticket_number, Final_MIN(min$0) AS EXPR$0])
   :     +- Exchange(distribution=[hash[ss_ticket_number]])
   :        +- LocalHashAggregate(groupBy=[ss_ticket_number], select=[ss_ticket_number, Partial_MIN(ss_promo_sk) AS min$0])
   :           +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cr_returning_addr_sk, ss_sold_time_sk)], select=[cr_returning_addr_sk, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
   :              :- Exchange(distribution=[broadcast])
   :              :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_returning_addr_sk], metadata=[]]], fields=[cr_returning_addr_sk])
   :              +- Sort(orderBy=[ss_ext_wholesale_cost ASC])
   :                 +- Exchange(distribution=[single])
   :                    +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
   +- Exchange(distribution=[broadcast])
      +- Calc(select=[sm_carrier_node_14, c_first_sales_date_sk_node_15])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(sm_ship_mode_sk_node_14, c_current_cdemo_sk_node_15)], select=[sm_ship_mode_sk_node_14, sm_carrier_node_14, c_current_cdemo_sk_node_15, c_first_sales_date_sk_node_15], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[sm_ship_mode_sk AS sm_ship_mode_sk_node_14, sm_carrier AS sm_carrier_node_14])
            :     +- HashAggregate(isMerge=[true], groupBy=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
            :        +- Exchange(distribution=[hash[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract]])
            :           +- LocalHashAggregate(groupBy=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
            :              +- Calc(select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], where=[<(CHAR_LENGTH(sm_code), 5)])
            :                 +- TableSourceScan(table=[[default_catalog, default_database, ship_mode, filter=[<(CHAR_LENGTH(sm_code), 5)]]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
            +- Calc(select=[c_current_cdemo_sk AS c_current_cdemo_sk_node_15, c_first_sales_date_sk AS c_first_sales_date_sk_node_15])
               +- Sort(orderBy=[c_birth_country ASC])
                  +- Exchange(distribution=[single])
                     +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

== Optimized Execution Plan ==
Calc(select=[sm_carrier_node_14])
+- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(ss_promo_sk_node_13 = c_first_sales_date_sk_node_15)], select=[ss_promo_sk_node_13, sm_carrier_node_14, c_first_sales_date_sk_node_15], isBroadcast=[true], build=[right])\
:- Calc(select=[EXPR$0 AS ss_promo_sk_node_13])\
:  +- HashAggregate(isMerge=[true], groupBy=[ss_ticket_number], select=[ss_ticket_number, Final_MIN(min$0) AS EXPR$0])\
:     +- [#2] Exchange(distribution=[hash[ss_ticket_number]])\
+- [#1] Exchange(distribution=[broadcast])\
])
   :- Exchange(distribution=[broadcast])
   :  +- Calc(select=[sm_carrier_node_14, c_first_sales_date_sk_node_15])
   :     +- NestedLoopJoin(joinType=[InnerJoin], where=[(sm_ship_mode_sk_node_14 = c_current_cdemo_sk_node_15)], select=[sm_ship_mode_sk_node_14, sm_carrier_node_14, c_current_cdemo_sk_node_15, c_first_sales_date_sk_node_15], build=[left])
   :        :- Exchange(distribution=[broadcast])
   :        :  +- Calc(select=[sm_ship_mode_sk AS sm_ship_mode_sk_node_14, sm_carrier AS sm_carrier_node_14])
   :        :     +- HashAggregate(isMerge=[true], groupBy=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
   :        :        +- Exchange(distribution=[hash[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract]])
   :        :           +- LocalHashAggregate(groupBy=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
   :        :              +- Calc(select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], where=[(CHAR_LENGTH(sm_code) < 5)])
   :        :                 +- TableSourceScan(table=[[default_catalog, default_database, ship_mode, filter=[<(CHAR_LENGTH(sm_code), 5)]]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
   :        +- Calc(select=[c_current_cdemo_sk AS c_current_cdemo_sk_node_15, c_first_sales_date_sk AS c_first_sales_date_sk_node_15])
   :           +- Sort(orderBy=[c_birth_country ASC])
   :              +- Exchange(distribution=[single])
   :                 +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
   +- Exchange(distribution=[hash[ss_ticket_number]])
      +- LocalHashAggregate(groupBy=[ss_ticket_number], select=[ss_ticket_number, Partial_MIN(ss_promo_sk) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(cr_returning_addr_sk = ss_sold_time_sk)], select=[cr_returning_addr_sk, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_returning_addr_sk], metadata=[]]], fields=[cr_returning_addr_sk])
            +- Sort(orderBy=[ss_ext_wholesale_cost ASC])
               +- Exchange(distribution=[single])
                  +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o296189807.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#597294851:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[16](input=RelSubset#597294849,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[16]), rel#597294848:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[16](input=RelSubset#597294847,groupBy=ss_sold_time_sk, ss_ticket_number,select=ss_sold_time_sk, ss_ticket_number, Partial_MIN(ss_promo_sk) AS min$0)]
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