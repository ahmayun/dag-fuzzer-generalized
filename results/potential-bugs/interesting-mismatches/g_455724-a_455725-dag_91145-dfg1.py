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
    return values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_12 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_11 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_14 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_9 = autonode_12.join(autonode_13, col('c_birth_year_node_12') == col('sm_ship_mode_sk_node_13'))
autonode_8 = autonode_11.alias('GNjTP')
autonode_10 = autonode_14.alias('dUnA6')
autonode_6 = autonode_8.join(autonode_9, col('sm_type_node_13') == col('i_manufact_node_11'))
autonode_7 = autonode_10.select(col('ss_coupon_amt_node_14'))
autonode_4 = autonode_6.order_by(col('c_birth_year_node_12'))
autonode_5 = autonode_7.order_by(col('ss_coupon_amt_node_14'))
autonode_2 = autonode_4.add_columns(lit("hello"))
autonode_3 = autonode_5.select(col('ss_coupon_amt_node_14'))
autonode_1 = autonode_2.join(autonode_3, col('i_current_price_node_11') == col('ss_coupon_amt_node_14'))
sink = autonode_1.group_by(col('c_preferred_cust_flag_node_12')).select(col('c_first_shipto_date_sk_node_12').max.alias('c_first_shipto_date_sk_node_12'))
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
      "error_message": "An error occurred while calling o248110962.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#502060773:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[35](input=RelSubset#502060771,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[35]), rel#502060770:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[35](input=RelSubset#502060769,groupBy=i_current_price_node_11, c_preferred_cust_flag,select=i_current_price_node_11, c_preferred_cust_flag, Partial_MAX(c_first_shipto_date_sk) AS max$0)]
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
LogicalProject(c_first_shipto_date_sk_node_12=[$1])
+- LogicalAggregate(group=[{32}], EXPR$0=[MAX($27)])
   +- LogicalJoin(condition=[=($5, $47)], joinType=[inner])
      :- LogicalProject(GNjTP=[$0], i_item_id_node_11=[$1], i_rec_start_date_node_11=[$2], i_rec_end_date_node_11=[$3], i_item_desc_node_11=[$4], i_current_price_node_11=[$5], i_wholesale_cost_node_11=[$6], i_brand_id_node_11=[$7], i_brand_node_11=[$8], i_class_id_node_11=[$9], i_class_node_11=[$10], i_category_id_node_11=[$11], i_category_node_11=[$12], i_manufact_id_node_11=[$13], i_manufact_node_11=[$14], i_size_node_11=[$15], i_formulation_node_11=[$16], i_color_node_11=[$17], i_units_node_11=[$18], i_container_node_11=[$19], i_manager_id_node_11=[$20], i_product_name_node_11=[$21], c_customer_sk_node_12=[$22], c_customer_id_node_12=[$23], c_current_cdemo_sk_node_12=[$24], c_current_hdemo_sk_node_12=[$25], c_current_addr_sk_node_12=[$26], c_first_shipto_date_sk_node_12=[$27], c_first_sales_date_sk_node_12=[$28], c_salutation_node_12=[$29], c_first_name_node_12=[$30], c_last_name_node_12=[$31], c_preferred_cust_flag_node_12=[$32], c_birth_day_node_12=[$33], c_birth_month_node_12=[$34], c_birth_year_node_12=[$35], c_birth_country_node_12=[$36], c_login_node_12=[$37], c_email_address_node_12=[$38], c_last_review_date_node_12=[$39], sm_ship_mode_sk_node_13=[$40], sm_ship_mode_id_node_13=[$41], sm_type_node_13=[$42], sm_code_node_13=[$43], sm_carrier_node_13=[$44], sm_contract_node_13=[$45], _c46=[_UTF-16LE'hello'])
      :  +- LogicalSort(sort0=[$35], dir0=[ASC])
      :     +- LogicalJoin(condition=[=($42, $14)], joinType=[inner])
      :        :- LogicalProject(GNjTP=[AS($0, _UTF-16LE'GNjTP')], i_item_id_node_11=[$1], i_rec_start_date_node_11=[$2], i_rec_end_date_node_11=[$3], i_item_desc_node_11=[$4], i_current_price_node_11=[$5], i_wholesale_cost_node_11=[$6], i_brand_id_node_11=[$7], i_brand_node_11=[$8], i_class_id_node_11=[$9], i_class_node_11=[$10], i_category_id_node_11=[$11], i_category_node_11=[$12], i_manufact_id_node_11=[$13], i_manufact_node_11=[$14], i_size_node_11=[$15], i_formulation_node_11=[$16], i_color_node_11=[$17], i_units_node_11=[$18], i_container_node_11=[$19], i_manager_id_node_11=[$20], i_product_name_node_11=[$21])
      :        :  +- LogicalTableScan(table=[[default_catalog, default_database, item]])
      :        +- LogicalJoin(condition=[=($13, $18)], joinType=[inner])
      :           :- LogicalProject(c_customer_sk_node_12=[$0], c_customer_id_node_12=[$1], c_current_cdemo_sk_node_12=[$2], c_current_hdemo_sk_node_12=[$3], c_current_addr_sk_node_12=[$4], c_first_shipto_date_sk_node_12=[$5], c_first_sales_date_sk_node_12=[$6], c_salutation_node_12=[$7], c_first_name_node_12=[$8], c_last_name_node_12=[$9], c_preferred_cust_flag_node_12=[$10], c_birth_day_node_12=[$11], c_birth_month_node_12=[$12], c_birth_year_node_12=[$13], c_birth_country_node_12=[$14], c_login_node_12=[$15], c_email_address_node_12=[$16], c_last_review_date_node_12=[$17])
      :           :  +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
      :           +- LogicalProject(sm_ship_mode_sk_node_13=[$0], sm_ship_mode_id_node_13=[$1], sm_type_node_13=[$2], sm_code_node_13=[$3], sm_carrier_node_13=[$4], sm_contract_node_13=[$5])
      :              +- LogicalTableScan(table=[[default_catalog, default_database, ship_mode]])
      +- LogicalProject(ss_coupon_amt_node_14=[$0])
         +- LogicalSort(sort0=[$0], dir0=[ASC])
            +- LogicalProject(ss_coupon_amt_node_14=[$19])
               +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS c_first_shipto_date_sk_node_12])
+- SortAggregate(isMerge=[false], groupBy=[c_preferred_cust_flag_node_12], select=[c_preferred_cust_flag_node_12, MAX(c_first_shipto_date_sk_node_12) AS EXPR$0])
   +- Sort(orderBy=[c_preferred_cust_flag_node_12 ASC])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(i_current_price_node_11, ss_coupon_amt)], select=[GNjTP, i_item_id_node_11, i_rec_start_date_node_11, i_rec_end_date_node_11, i_item_desc_node_11, i_current_price_node_11, i_wholesale_cost_node_11, i_brand_id_node_11, i_brand_node_11, i_class_id_node_11, i_class_node_11, i_category_id_node_11, i_category_node_11, i_manufact_id_node_11, i_manufact_node_11, i_size_node_11, i_formulation_node_11, i_color_node_11, i_units_node_11, i_container_node_11, i_manager_id_node_11, i_product_name_node_11, c_customer_sk_node_12, c_customer_id_node_12, c_current_cdemo_sk_node_12, c_current_hdemo_sk_node_12, c_current_addr_sk_node_12, c_first_shipto_date_sk_node_12, c_first_sales_date_sk_node_12, c_salutation_node_12, c_first_name_node_12, c_last_name_node_12, c_preferred_cust_flag_node_12, c_birth_day_node_12, c_birth_month_node_12, c_birth_year_node_12, c_birth_country_node_12, c_login_node_12, c_email_address_node_12, c_last_review_date_node_12, sm_ship_mode_sk_node_13, sm_ship_mode_id_node_13, sm_type_node_13, sm_code_node_13, sm_carrier_node_13, sm_contract_node_13, _c46, ss_coupon_amt], build=[right])
         :- Exchange(distribution=[hash[c_preferred_cust_flag_node_12]])
         :  +- Calc(select=[GNjTP, i_item_id_node_11, i_rec_start_date_node_11, i_rec_end_date_node_11, i_item_desc_node_11, i_current_price_node_11, i_wholesale_cost_node_11, i_brand_id_node_11, i_brand_node_11, i_class_id_node_11, i_class_node_11, i_category_id_node_11, i_category_node_11, i_manufact_id_node_11, i_manufact_node_11, i_size_node_11, i_formulation_node_11, i_color_node_11, i_units_node_11, i_container_node_11, i_manager_id_node_11, i_product_name_node_11, c_customer_sk AS c_customer_sk_node_12, c_customer_id AS c_customer_id_node_12, c_current_cdemo_sk AS c_current_cdemo_sk_node_12, c_current_hdemo_sk AS c_current_hdemo_sk_node_12, c_current_addr_sk AS c_current_addr_sk_node_12, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_12, c_first_sales_date_sk AS c_first_sales_date_sk_node_12, c_salutation AS c_salutation_node_12, c_first_name AS c_first_name_node_12, c_last_name AS c_last_name_node_12, c_preferred_cust_flag AS c_preferred_cust_flag_node_12, c_birth_day AS c_birth_day_node_12, c_birth_month AS c_birth_month_node_12, c_birth_year AS c_birth_year_node_12, c_birth_country AS c_birth_country_node_12, c_login AS c_login_node_12, c_email_address AS c_email_address_node_12, c_last_review_date AS c_last_review_date_node_12, sm_ship_mode_sk AS sm_ship_mode_sk_node_13, sm_ship_mode_id AS sm_ship_mode_id_node_13, sm_type AS sm_type_node_13, sm_code AS sm_code_node_13, sm_carrier AS sm_carrier_node_13, sm_contract AS sm_contract_node_13, 'hello' AS _c46])
         :     +- SortLimit(orderBy=[c_birth_year ASC], offset=[0], fetch=[1], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[c_birth_year ASC], offset=[0], fetch=[1], global=[false])
         :              +- HashJoin(joinType=[InnerJoin], where=[=(sm_type, i_manufact_node_11)], select=[GNjTP, i_item_id_node_11, i_rec_start_date_node_11, i_rec_end_date_node_11, i_item_desc_node_11, i_current_price_node_11, i_wholesale_cost_node_11, i_brand_id_node_11, i_brand_node_11, i_class_id_node_11, i_class_node_11, i_category_id_node_11, i_category_node_11, i_manufact_id_node_11, i_manufact_node_11, i_size_node_11, i_formulation_node_11, i_color_node_11, i_units_node_11, i_container_node_11, i_manager_id_node_11, i_product_name_node_11, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], build=[right])
         :                 :- Exchange(distribution=[hash[i_manufact_node_11]])
         :                 :  +- Calc(select=[i_item_sk AS GNjTP, i_item_id AS i_item_id_node_11, i_rec_start_date AS i_rec_start_date_node_11, i_rec_end_date AS i_rec_end_date_node_11, i_item_desc AS i_item_desc_node_11, i_current_price AS i_current_price_node_11, i_wholesale_cost AS i_wholesale_cost_node_11, i_brand_id AS i_brand_id_node_11, i_brand AS i_brand_node_11, i_class_id AS i_class_id_node_11, i_class AS i_class_node_11, i_category_id AS i_category_id_node_11, i_category AS i_category_node_11, i_manufact_id AS i_manufact_id_node_11, i_manufact AS i_manufact_node_11, i_size AS i_size_node_11, i_formulation AS i_formulation_node_11, i_color AS i_color_node_11, i_units AS i_units_node_11, i_container AS i_container_node_11, i_manager_id AS i_manager_id_node_11, i_product_name AS i_product_name_node_11])
         :                 :     +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
         :                 +- Exchange(distribution=[hash[sm_type]])
         :                    +- HashJoin(joinType=[InnerJoin], where=[=(c_birth_year, sm_ship_mode_sk)], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], isBroadcast=[true], build=[right])
         :                       :- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
         :                       +- Exchange(distribution=[broadcast])
         :                          +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
         +- Exchange(distribution=[broadcast])
            +- SortLimit(orderBy=[ss_coupon_amt ASC], offset=[0], fetch=[1], global=[true])
               +- Exchange(distribution=[single])
                  +- SortLimit(orderBy=[ss_coupon_amt ASC], offset=[0], fetch=[1], global=[false])
                     +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_coupon_amt], metadata=[]]], fields=[ss_coupon_amt])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS c_first_shipto_date_sk_node_12])
+- SortAggregate(isMerge=[false], groupBy=[c_preferred_cust_flag_node_12], select=[c_preferred_cust_flag_node_12, MAX(c_first_shipto_date_sk_node_12) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[c_preferred_cust_flag_node_12 ASC])
         +- Exchange(distribution=[keep_input_as_is[hash[c_preferred_cust_flag_node_12]]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(i_current_price_node_11 = ss_coupon_amt)], select=[GNjTP, i_item_id_node_11, i_rec_start_date_node_11, i_rec_end_date_node_11, i_item_desc_node_11, i_current_price_node_11, i_wholesale_cost_node_11, i_brand_id_node_11, i_brand_node_11, i_class_id_node_11, i_class_node_11, i_category_id_node_11, i_category_node_11, i_manufact_id_node_11, i_manufact_node_11, i_size_node_11, i_formulation_node_11, i_color_node_11, i_units_node_11, i_container_node_11, i_manager_id_node_11, i_product_name_node_11, c_customer_sk_node_12, c_customer_id_node_12, c_current_cdemo_sk_node_12, c_current_hdemo_sk_node_12, c_current_addr_sk_node_12, c_first_shipto_date_sk_node_12, c_first_sales_date_sk_node_12, c_salutation_node_12, c_first_name_node_12, c_last_name_node_12, c_preferred_cust_flag_node_12, c_birth_day_node_12, c_birth_month_node_12, c_birth_year_node_12, c_birth_country_node_12, c_login_node_12, c_email_address_node_12, c_last_review_date_node_12, sm_ship_mode_sk_node_13, sm_ship_mode_id_node_13, sm_type_node_13, sm_code_node_13, sm_carrier_node_13, sm_contract_node_13, _c46, ss_coupon_amt], build=[right])
               :- Exchange(distribution=[hash[c_preferred_cust_flag_node_12]])
               :  +- Calc(select=[GNjTP, i_item_id_node_11, i_rec_start_date_node_11, i_rec_end_date_node_11, i_item_desc_node_11, i_current_price_node_11, i_wholesale_cost_node_11, i_brand_id_node_11, i_brand_node_11, i_class_id_node_11, i_class_node_11, i_category_id_node_11, i_category_node_11, i_manufact_id_node_11, i_manufact_node_11, i_size_node_11, i_formulation_node_11, i_color_node_11, i_units_node_11, i_container_node_11, i_manager_id_node_11, i_product_name_node_11, c_customer_sk AS c_customer_sk_node_12, c_customer_id AS c_customer_id_node_12, c_current_cdemo_sk AS c_current_cdemo_sk_node_12, c_current_hdemo_sk AS c_current_hdemo_sk_node_12, c_current_addr_sk AS c_current_addr_sk_node_12, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_12, c_first_sales_date_sk AS c_first_sales_date_sk_node_12, c_salutation AS c_salutation_node_12, c_first_name AS c_first_name_node_12, c_last_name AS c_last_name_node_12, c_preferred_cust_flag AS c_preferred_cust_flag_node_12, c_birth_day AS c_birth_day_node_12, c_birth_month AS c_birth_month_node_12, c_birth_year AS c_birth_year_node_12, c_birth_country AS c_birth_country_node_12, c_login AS c_login_node_12, c_email_address AS c_email_address_node_12, c_last_review_date AS c_last_review_date_node_12, sm_ship_mode_sk AS sm_ship_mode_sk_node_13, sm_ship_mode_id AS sm_ship_mode_id_node_13, sm_type AS sm_type_node_13, sm_code AS sm_code_node_13, sm_carrier AS sm_carrier_node_13, sm_contract AS sm_contract_node_13, 'hello' AS _c46])
               :     +- SortLimit(orderBy=[c_birth_year ASC], offset=[0], fetch=[1], global=[true])
               :        +- Exchange(distribution=[single])
               :           +- SortLimit(orderBy=[c_birth_year ASC], offset=[0], fetch=[1], global=[false])
               :              +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(sm_type = i_manufact_node_11)], select=[GNjTP, i_item_id_node_11, i_rec_start_date_node_11, i_rec_end_date_node_11, i_item_desc_node_11, i_current_price_node_11, i_wholesale_cost_node_11, i_brand_id_node_11, i_brand_node_11, i_class_id_node_11, i_class_node_11, i_category_id_node_11, i_category_node_11, i_manufact_id_node_11, i_manufact_node_11, i_size_node_11, i_formulation_node_11, i_color_node_11, i_units_node_11, i_container_node_11, i_manager_id_node_11, i_product_name_node_11, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], build=[right])
               :                 :- Exchange(distribution=[hash[i_manufact_node_11]])
               :                 :  +- Calc(select=[i_item_sk AS GNjTP, i_item_id AS i_item_id_node_11, i_rec_start_date AS i_rec_start_date_node_11, i_rec_end_date AS i_rec_end_date_node_11, i_item_desc AS i_item_desc_node_11, i_current_price AS i_current_price_node_11, i_wholesale_cost AS i_wholesale_cost_node_11, i_brand_id AS i_brand_id_node_11, i_brand AS i_brand_node_11, i_class_id AS i_class_id_node_11, i_class AS i_class_node_11, i_category_id AS i_category_id_node_11, i_category AS i_category_node_11, i_manufact_id AS i_manufact_id_node_11, i_manufact AS i_manufact_node_11, i_size AS i_size_node_11, i_formulation AS i_formulation_node_11, i_color AS i_color_node_11, i_units AS i_units_node_11, i_container AS i_container_node_11, i_manager_id AS i_manager_id_node_11, i_product_name AS i_product_name_node_11])
               :                 :     +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
               :                 +- Exchange(distribution=[hash[sm_type]])
               :                    +- HashJoin(joinType=[InnerJoin], where=[(c_birth_year = sm_ship_mode_sk)], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], isBroadcast=[true], build=[right])
               :                       :- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
               :                       +- Exchange(distribution=[broadcast])
               :                          +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
               +- Exchange(distribution=[broadcast])
                  +- SortLimit(orderBy=[ss_coupon_amt ASC], offset=[0], fetch=[1], global=[true])
                     +- Exchange(distribution=[single])
                        +- SortLimit(orderBy=[ss_coupon_amt ASC], offset=[0], fetch=[1], global=[false])
                           +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_coupon_amt], metadata=[]]], fields=[ss_coupon_amt])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0