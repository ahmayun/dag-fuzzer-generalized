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

autonode_13 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_12 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_14 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_15 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_9 = autonode_12.order_by(col('ws_coupon_amt_node_12'))
autonode_10 = autonode_13.join(autonode_14, col('t_minute_node_13') == col('c_current_addr_sk_node_14'))
autonode_11 = autonode_15.filter(col('sm_contract_node_15').char_length > 5)
autonode_6 = autonode_9.alias('YRFAM')
autonode_7 = autonode_10.distinct()
autonode_8 = autonode_11.select(col('sm_type_node_15'))
autonode_4 = autonode_6.add_columns(lit("hello"))
autonode_5 = autonode_7.join(autonode_8, col('sm_type_node_15') == col('c_first_name_node_14'))
autonode_3 = autonode_4.join(autonode_5, col('ws_ship_mode_sk_node_12') == col('c_current_cdemo_sk_node_14'))
autonode_2 = autonode_3.group_by(col('t_meal_time_node_13')).select(col('c_birth_month_node_14').count.alias('c_birth_month_node_14'))
autonode_1 = autonode_2.select(col('c_birth_month_node_14'))
sink = autonode_1.distinct()
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
      "error_message": "An error occurred while calling o310912423.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#626388291:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[27](input=RelSubset#626388289,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[27]), rel#626388288:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[27](input=RelSubset#626388287,groupBy=ws_ship_mode_sk,select=ws_ship_mode_sk, Partial_COUNT(*) AS count1$0)]
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
    },
    "unopt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalAggregate(group=[{0}])
+- LogicalProject(c_birth_month_node_14=[$1])
   +- LogicalAggregate(group=[{44}], EXPR$0=[COUNT($57)])
      +- LogicalJoin(condition=[=($14, $47)], joinType=[inner])
         :- LogicalProject(YRFAM=[AS($0, _UTF-16LE'YRFAM')], ws_sold_time_sk_node_12=[$1], ws_ship_date_sk_node_12=[$2], ws_item_sk_node_12=[$3], ws_bill_customer_sk_node_12=[$4], ws_bill_cdemo_sk_node_12=[$5], ws_bill_hdemo_sk_node_12=[$6], ws_bill_addr_sk_node_12=[$7], ws_ship_customer_sk_node_12=[$8], ws_ship_cdemo_sk_node_12=[$9], ws_ship_hdemo_sk_node_12=[$10], ws_ship_addr_sk_node_12=[$11], ws_web_page_sk_node_12=[$12], ws_web_site_sk_node_12=[$13], ws_ship_mode_sk_node_12=[$14], ws_warehouse_sk_node_12=[$15], ws_promo_sk_node_12=[$16], ws_order_number_node_12=[$17], ws_quantity_node_12=[$18], ws_wholesale_cost_node_12=[$19], ws_list_price_node_12=[$20], ws_sales_price_node_12=[$21], ws_ext_discount_amt_node_12=[$22], ws_ext_sales_price_node_12=[$23], ws_ext_wholesale_cost_node_12=[$24], ws_ext_list_price_node_12=[$25], ws_ext_tax_node_12=[$26], ws_coupon_amt_node_12=[$27], ws_ext_ship_cost_node_12=[$28], ws_net_paid_node_12=[$29], ws_net_paid_inc_tax_node_12=[$30], ws_net_paid_inc_ship_node_12=[$31], ws_net_paid_inc_ship_tax_node_12=[$32], ws_net_profit_node_12=[$33], _c34=[_UTF-16LE'hello'])
         :  +- LogicalSort(sort0=[$27], dir0=[ASC])
         :     +- LogicalProject(ws_sold_date_sk_node_12=[$0], ws_sold_time_sk_node_12=[$1], ws_ship_date_sk_node_12=[$2], ws_item_sk_node_12=[$3], ws_bill_customer_sk_node_12=[$4], ws_bill_cdemo_sk_node_12=[$5], ws_bill_hdemo_sk_node_12=[$6], ws_bill_addr_sk_node_12=[$7], ws_ship_customer_sk_node_12=[$8], ws_ship_cdemo_sk_node_12=[$9], ws_ship_hdemo_sk_node_12=[$10], ws_ship_addr_sk_node_12=[$11], ws_web_page_sk_node_12=[$12], ws_web_site_sk_node_12=[$13], ws_ship_mode_sk_node_12=[$14], ws_warehouse_sk_node_12=[$15], ws_promo_sk_node_12=[$16], ws_order_number_node_12=[$17], ws_quantity_node_12=[$18], ws_wholesale_cost_node_12=[$19], ws_list_price_node_12=[$20], ws_sales_price_node_12=[$21], ws_ext_discount_amt_node_12=[$22], ws_ext_sales_price_node_12=[$23], ws_ext_wholesale_cost_node_12=[$24], ws_ext_list_price_node_12=[$25], ws_ext_tax_node_12=[$26], ws_coupon_amt_node_12=[$27], ws_ext_ship_cost_node_12=[$28], ws_net_paid_node_12=[$29], ws_net_paid_inc_tax_node_12=[$30], ws_net_paid_inc_ship_node_12=[$31], ws_net_paid_inc_ship_tax_node_12=[$32], ws_net_profit_node_12=[$33])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
         +- LogicalJoin(condition=[=($28, $18)], joinType=[inner])
            :- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27}])
            :  +- LogicalJoin(condition=[=($4, $14)], joinType=[inner])
            :     :- LogicalProject(t_time_sk_node_13=[$0], t_time_id_node_13=[$1], t_time_node_13=[$2], t_hour_node_13=[$3], t_minute_node_13=[$4], t_second_node_13=[$5], t_am_pm_node_13=[$6], t_shift_node_13=[$7], t_sub_shift_node_13=[$8], t_meal_time_node_13=[$9])
            :     :  +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
            :     +- LogicalProject(c_customer_sk_node_14=[$0], c_customer_id_node_14=[$1], c_current_cdemo_sk_node_14=[$2], c_current_hdemo_sk_node_14=[$3], c_current_addr_sk_node_14=[$4], c_first_shipto_date_sk_node_14=[$5], c_first_sales_date_sk_node_14=[$6], c_salutation_node_14=[$7], c_first_name_node_14=[$8], c_last_name_node_14=[$9], c_preferred_cust_flag_node_14=[$10], c_birth_day_node_14=[$11], c_birth_month_node_14=[$12], c_birth_year_node_14=[$13], c_birth_country_node_14=[$14], c_login_node_14=[$15], c_email_address_node_14=[$16], c_last_review_date_node_14=[$17])
            :        +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
            +- LogicalProject(sm_type_node_15=[$2])
               +- LogicalFilter(condition=[>(CHAR_LENGTH($5), 5)])
                  +- LogicalProject(sm_ship_mode_sk_node_15=[$0], sm_ship_mode_id_node_15=[$1], sm_type_node_15=[$2], sm_code_node_15=[$3], sm_carrier_node_15=[$4], sm_contract_node_15=[$5])
                     +- LogicalTableScan(table=[[default_catalog, default_database, ship_mode]])

== Optimized Physical Plan ==
HashAggregate(isMerge=[true], groupBy=[c_birth_month_node_14], select=[c_birth_month_node_14])
+- Exchange(distribution=[hash[c_birth_month_node_14]])
   +- LocalHashAggregate(groupBy=[c_birth_month_node_14], select=[c_birth_month_node_14])
      +- Calc(select=[EXPR$0 AS c_birth_month_node_14])
         +- HashAggregate(isMerge=[false], groupBy=[t_meal_time], select=[t_meal_time, COUNT(c_birth_month) AS EXPR$0])
            +- Exchange(distribution=[hash[t_meal_time]])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ws_ship_mode_sk_node_12, c_current_cdemo_sk)], select=[YRFAM, ws_sold_time_sk_node_12, ws_ship_date_sk_node_12, ws_item_sk_node_12, ws_bill_customer_sk_node_12, ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk_node_12, ws_bill_addr_sk_node_12, ws_ship_customer_sk_node_12, ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk_node_12, ws_ship_addr_sk_node_12, ws_web_page_sk_node_12, ws_web_site_sk_node_12, ws_ship_mode_sk_node_12, ws_warehouse_sk_node_12, ws_promo_sk_node_12, ws_order_number_node_12, ws_quantity_node_12, ws_wholesale_cost_node_12, ws_list_price_node_12, ws_sales_price_node_12, ws_ext_discount_amt_node_12, ws_ext_sales_price_node_12, ws_ext_wholesale_cost_node_12, ws_ext_list_price_node_12, ws_ext_tax_node_12, ws_coupon_amt_node_12, ws_ext_ship_cost_node_12, ws_net_paid_node_12, ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax_node_12, ws_net_profit_node_12, _c34, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, sm_type_node_15], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[ws_sold_date_sk AS YRFAM, ws_sold_time_sk AS ws_sold_time_sk_node_12, ws_ship_date_sk AS ws_ship_date_sk_node_12, ws_item_sk AS ws_item_sk_node_12, ws_bill_customer_sk AS ws_bill_customer_sk_node_12, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_12, ws_bill_addr_sk AS ws_bill_addr_sk_node_12, ws_ship_customer_sk AS ws_ship_customer_sk_node_12, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_12, ws_ship_addr_sk AS ws_ship_addr_sk_node_12, ws_web_page_sk AS ws_web_page_sk_node_12, ws_web_site_sk AS ws_web_site_sk_node_12, ws_ship_mode_sk AS ws_ship_mode_sk_node_12, ws_warehouse_sk AS ws_warehouse_sk_node_12, ws_promo_sk AS ws_promo_sk_node_12, ws_order_number AS ws_order_number_node_12, ws_quantity AS ws_quantity_node_12, ws_wholesale_cost AS ws_wholesale_cost_node_12, ws_list_price AS ws_list_price_node_12, ws_sales_price AS ws_sales_price_node_12, ws_ext_discount_amt AS ws_ext_discount_amt_node_12, ws_ext_sales_price AS ws_ext_sales_price_node_12, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_12, ws_ext_list_price AS ws_ext_list_price_node_12, ws_ext_tax AS ws_ext_tax_node_12, ws_coupon_amt AS ws_coupon_amt_node_12, ws_ext_ship_cost AS ws_ext_ship_cost_node_12, ws_net_paid AS ws_net_paid_node_12, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_12, ws_net_profit AS ws_net_profit_node_12, 'hello' AS _c34])
                  :     +- SortLimit(orderBy=[ws_coupon_amt ASC], offset=[0], fetch=[1], global=[true])
                  :        +- Exchange(distribution=[single])
                  :           +- SortLimit(orderBy=[ws_coupon_amt ASC], offset=[0], fetch=[1], global=[false])
                  :              +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                  +- HashJoin(joinType=[InnerJoin], where=[=(sm_type_node_15, c_first_name)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, sm_type_node_15], isBroadcast=[true], build=[right])
                     :- HashAggregate(isMerge=[false], groupBy=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                     :  +- HashJoin(joinType=[InnerJoin], where=[=(t_minute, c_current_addr_sk)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[left])
                     :     :- Exchange(distribution=[hash[t_minute]])
                     :     :  +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
                     :     +- Exchange(distribution=[hash[c_current_addr_sk]])
                     :        +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                     +- Exchange(distribution=[broadcast])
                        +- Calc(select=[sm_type AS sm_type_node_15], where=[>(CHAR_LENGTH(sm_contract), 5)])
                           +- TableSourceScan(table=[[default_catalog, default_database, ship_mode, filter=[>(CHAR_LENGTH(sm_contract), 5)], project=[sm_type, sm_contract], metadata=[]]], fields=[sm_type, sm_contract])

== Optimized Execution Plan ==
HashAggregate(isMerge=[true], groupBy=[c_birth_month_node_14], select=[c_birth_month_node_14])
+- Exchange(distribution=[hash[c_birth_month_node_14]])
   +- LocalHashAggregate(groupBy=[c_birth_month_node_14], select=[c_birth_month_node_14])
      +- Calc(select=[EXPR$0 AS c_birth_month_node_14])
         +- HashAggregate(isMerge=[false], groupBy=[t_meal_time], select=[t_meal_time, COUNT(c_birth_month) AS EXPR$0])
            +- Exchange(distribution=[hash[t_meal_time]])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[(ws_ship_mode_sk_node_12 = c_current_cdemo_sk)], select=[YRFAM, ws_sold_time_sk_node_12, ws_ship_date_sk_node_12, ws_item_sk_node_12, ws_bill_customer_sk_node_12, ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk_node_12, ws_bill_addr_sk_node_12, ws_ship_customer_sk_node_12, ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk_node_12, ws_ship_addr_sk_node_12, ws_web_page_sk_node_12, ws_web_site_sk_node_12, ws_ship_mode_sk_node_12, ws_warehouse_sk_node_12, ws_promo_sk_node_12, ws_order_number_node_12, ws_quantity_node_12, ws_wholesale_cost_node_12, ws_list_price_node_12, ws_sales_price_node_12, ws_ext_discount_amt_node_12, ws_ext_sales_price_node_12, ws_ext_wholesale_cost_node_12, ws_ext_list_price_node_12, ws_ext_tax_node_12, ws_coupon_amt_node_12, ws_ext_ship_cost_node_12, ws_net_paid_node_12, ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax_node_12, ws_net_profit_node_12, _c34, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, sm_type_node_15], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[ws_sold_date_sk AS YRFAM, ws_sold_time_sk AS ws_sold_time_sk_node_12, ws_ship_date_sk AS ws_ship_date_sk_node_12, ws_item_sk AS ws_item_sk_node_12, ws_bill_customer_sk AS ws_bill_customer_sk_node_12, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_12, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_12, ws_bill_addr_sk AS ws_bill_addr_sk_node_12, ws_ship_customer_sk AS ws_ship_customer_sk_node_12, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_12, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_12, ws_ship_addr_sk AS ws_ship_addr_sk_node_12, ws_web_page_sk AS ws_web_page_sk_node_12, ws_web_site_sk AS ws_web_site_sk_node_12, ws_ship_mode_sk AS ws_ship_mode_sk_node_12, ws_warehouse_sk AS ws_warehouse_sk_node_12, ws_promo_sk AS ws_promo_sk_node_12, ws_order_number AS ws_order_number_node_12, ws_quantity AS ws_quantity_node_12, ws_wholesale_cost AS ws_wholesale_cost_node_12, ws_list_price AS ws_list_price_node_12, ws_sales_price AS ws_sales_price_node_12, ws_ext_discount_amt AS ws_ext_discount_amt_node_12, ws_ext_sales_price AS ws_ext_sales_price_node_12, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_12, ws_ext_list_price AS ws_ext_list_price_node_12, ws_ext_tax AS ws_ext_tax_node_12, ws_coupon_amt AS ws_coupon_amt_node_12, ws_ext_ship_cost AS ws_ext_ship_cost_node_12, ws_net_paid AS ws_net_paid_node_12, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_12, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_12, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_12, ws_net_profit AS ws_net_profit_node_12, 'hello' AS _c34])
                  :     +- SortLimit(orderBy=[ws_coupon_amt ASC], offset=[0], fetch=[1], global=[true])
                  :        +- Exchange(distribution=[single])
                  :           +- SortLimit(orderBy=[ws_coupon_amt ASC], offset=[0], fetch=[1], global=[false])
                  :              +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                  +- HashJoin(joinType=[InnerJoin], where=[(sm_type_node_15 = c_first_name)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, sm_type_node_15], isBroadcast=[true], build=[right])
                     :- HashAggregate(isMerge=[false], groupBy=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                     :  +- Exchange(distribution=[keep_input_as_is[hash[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date]]])
                     :     +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(t_minute = c_current_addr_sk)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[left])
                     :        :- Exchange(distribution=[hash[t_minute]])
                     :        :  +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
                     :        +- Exchange(distribution=[hash[c_current_addr_sk]])
                     :           +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                     +- Exchange(distribution=[broadcast])
                        +- Calc(select=[sm_type AS sm_type_node_15], where=[(CHAR_LENGTH(sm_contract) > 5)])
                           +- TableSourceScan(table=[[default_catalog, default_database, ship_mode, filter=[>(CHAR_LENGTH(sm_contract), 5)], project=[sm_type, sm_contract], metadata=[]]], fields=[sm_type, sm_contract])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0