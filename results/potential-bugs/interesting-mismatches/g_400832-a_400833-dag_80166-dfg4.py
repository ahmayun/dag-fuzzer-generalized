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

autonode_6 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_7 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_4 = autonode_6.add_columns(lit("hello"))
autonode_5 = autonode_7.order_by(col('cr_refunded_cash_node_7'))
autonode_2 = autonode_4.order_by(col('web_gmt_offset_node_6'))
autonode_3 = autonode_5.limit(42)
autonode_1 = autonode_2.join(autonode_3, col('cr_refunded_cdemo_sk_node_7') == col('web_close_date_sk_node_6'))
sink = autonode_1.group_by(col('web_county_node_6')).select(col('web_county_node_6').max.alias('web_county_node_6'))
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
LogicalProject(web_county_node_6=[$1])
+- LogicalAggregate(group=[{20}], EXPR$0=[MAX($20)])
   +- LogicalJoin(condition=[=($31, $6)], joinType=[inner])
      :- LogicalSort(sort0=[$24], dir0=[ASC])
      :  +- LogicalProject(web_site_sk_node_6=[$0], web_site_id_node_6=[$1], web_rec_start_date_node_6=[$2], web_rec_end_date_node_6=[$3], web_name_node_6=[$4], web_open_date_sk_node_6=[$5], web_close_date_sk_node_6=[$6], web_class_node_6=[$7], web_manager_node_6=[$8], web_mkt_id_node_6=[$9], web_mkt_class_node_6=[$10], web_mkt_desc_node_6=[$11], web_market_manager_node_6=[$12], web_company_id_node_6=[$13], web_company_name_node_6=[$14], web_street_number_node_6=[$15], web_street_name_node_6=[$16], web_street_type_node_6=[$17], web_suite_number_node_6=[$18], web_city_node_6=[$19], web_county_node_6=[$20], web_state_node_6=[$21], web_zip_node_6=[$22], web_country_node_6=[$23], web_gmt_offset_node_6=[$24], web_tax_percentage_node_6=[$25], _c26=[_UTF-16LE'hello'])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])
      +- LogicalSort(sort0=[$23], dir0=[ASC], fetch=[42])
         +- LogicalProject(cr_returned_date_sk_node_7=[$0], cr_returned_time_sk_node_7=[$1], cr_item_sk_node_7=[$2], cr_refunded_customer_sk_node_7=[$3], cr_refunded_cdemo_sk_node_7=[$4], cr_refunded_hdemo_sk_node_7=[$5], cr_refunded_addr_sk_node_7=[$6], cr_returning_customer_sk_node_7=[$7], cr_returning_cdemo_sk_node_7=[$8], cr_returning_hdemo_sk_node_7=[$9], cr_returning_addr_sk_node_7=[$10], cr_call_center_sk_node_7=[$11], cr_catalog_page_sk_node_7=[$12], cr_ship_mode_sk_node_7=[$13], cr_warehouse_sk_node_7=[$14], cr_reason_sk_node_7=[$15], cr_order_number_node_7=[$16], cr_return_quantity_node_7=[$17], cr_return_amount_node_7=[$18], cr_return_tax_node_7=[$19], cr_return_amt_inc_tax_node_7=[$20], cr_fee_node_7=[$21], cr_return_ship_cost_node_7=[$22], cr_refunded_cash_node_7=[$23], cr_reversed_charge_node_7=[$24], cr_store_credit_node_7=[$25], cr_net_loss_node_7=[$26])
            +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS web_county_node_6])
+- SortAggregate(isMerge=[true], groupBy=[web_county_node_6], select=[web_county_node_6, Final_MAX(max$0) AS EXPR$0])
   +- Sort(orderBy=[web_county_node_6 ASC])
      +- Exchange(distribution=[hash[web_county_node_6]])
         +- LocalSortAggregate(groupBy=[web_county_node_6], select=[web_county_node_6, Partial_MAX(web_county_node_6) AS max$0])
            +- Sort(orderBy=[web_county_node_6 ASC])
               +- HashJoin(joinType=[InnerJoin], where=[=(cr_refunded_cdemo_sk, web_close_date_sk_node_6)], select=[web_site_sk_node_6, web_site_id_node_6, web_rec_start_date_node_6, web_rec_end_date_node_6, web_name_node_6, web_open_date_sk_node_6, web_close_date_sk_node_6, web_class_node_6, web_manager_node_6, web_mkt_id_node_6, web_mkt_class_node_6, web_mkt_desc_node_6, web_market_manager_node_6, web_company_id_node_6, web_company_name_node_6, web_street_number_node_6, web_street_name_node_6, web_street_type_node_6, web_suite_number_node_6, web_city_node_6, web_county_node_6, web_state_node_6, web_zip_node_6, web_country_node_6, web_gmt_offset_node_6, web_tax_percentage_node_6, _c26, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], isBroadcast=[true], build=[right])
                  :- Sort(orderBy=[web_gmt_offset_node_6 ASC])
                  :  +- Calc(select=[web_site_sk AS web_site_sk_node_6, web_site_id AS web_site_id_node_6, web_rec_start_date AS web_rec_start_date_node_6, web_rec_end_date AS web_rec_end_date_node_6, web_name AS web_name_node_6, web_open_date_sk AS web_open_date_sk_node_6, web_close_date_sk AS web_close_date_sk_node_6, web_class AS web_class_node_6, web_manager AS web_manager_node_6, web_mkt_id AS web_mkt_id_node_6, web_mkt_class AS web_mkt_class_node_6, web_mkt_desc AS web_mkt_desc_node_6, web_market_manager AS web_market_manager_node_6, web_company_id AS web_company_id_node_6, web_company_name AS web_company_name_node_6, web_street_number AS web_street_number_node_6, web_street_name AS web_street_name_node_6, web_street_type AS web_street_type_node_6, web_suite_number AS web_suite_number_node_6, web_city AS web_city_node_6, web_county AS web_county_node_6, web_state AS web_state_node_6, web_zip AS web_zip_node_6, web_country AS web_country_node_6, web_gmt_offset AS web_gmt_offset_node_6, web_tax_percentage AS web_tax_percentage_node_6, 'hello' AS _c26])
                  :     +- Exchange(distribution=[single])
                  :        +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                  +- Exchange(distribution=[broadcast])
                     +- SortLimit(orderBy=[cr_refunded_cash ASC], offset=[0], fetch=[42], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[cr_refunded_cash ASC], offset=[0], fetch=[42], global=[false])
                              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS web_county_node_6])
+- SortAggregate(isMerge=[true], groupBy=[web_county_node_6], select=[web_county_node_6, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[web_county_node_6 ASC])
         +- Exchange(distribution=[hash[web_county_node_6]])
            +- LocalSortAggregate(groupBy=[web_county_node_6], select=[web_county_node_6, Partial_MAX(web_county_node_6) AS max$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[web_county_node_6 ASC])
                     +- HashJoin(joinType=[InnerJoin], where=[(cr_refunded_cdemo_sk = web_close_date_sk_node_6)], select=[web_site_sk_node_6, web_site_id_node_6, web_rec_start_date_node_6, web_rec_end_date_node_6, web_name_node_6, web_open_date_sk_node_6, web_close_date_sk_node_6, web_class_node_6, web_manager_node_6, web_mkt_id_node_6, web_mkt_class_node_6, web_mkt_desc_node_6, web_market_manager_node_6, web_company_id_node_6, web_company_name_node_6, web_street_number_node_6, web_street_name_node_6, web_street_type_node_6, web_suite_number_node_6, web_city_node_6, web_county_node_6, web_state_node_6, web_zip_node_6, web_country_node_6, web_gmt_offset_node_6, web_tax_percentage_node_6, _c26, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], isBroadcast=[true], build=[right])
                        :- Sort(orderBy=[web_gmt_offset_node_6 ASC])
                        :  +- Calc(select=[web_site_sk AS web_site_sk_node_6, web_site_id AS web_site_id_node_6, web_rec_start_date AS web_rec_start_date_node_6, web_rec_end_date AS web_rec_end_date_node_6, web_name AS web_name_node_6, web_open_date_sk AS web_open_date_sk_node_6, web_close_date_sk AS web_close_date_sk_node_6, web_class AS web_class_node_6, web_manager AS web_manager_node_6, web_mkt_id AS web_mkt_id_node_6, web_mkt_class AS web_mkt_class_node_6, web_mkt_desc AS web_mkt_desc_node_6, web_market_manager AS web_market_manager_node_6, web_company_id AS web_company_id_node_6, web_company_name AS web_company_name_node_6, web_street_number AS web_street_number_node_6, web_street_name AS web_street_name_node_6, web_street_type AS web_street_type_node_6, web_suite_number AS web_suite_number_node_6, web_city AS web_city_node_6, web_county AS web_county_node_6, web_state AS web_state_node_6, web_zip AS web_zip_node_6, web_country AS web_country_node_6, web_gmt_offset AS web_gmt_offset_node_6, web_tax_percentage AS web_tax_percentage_node_6, 'hello' AS _c26])
                        :     +- Exchange(distribution=[single])
                        :        +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                        +- Exchange(distribution=[broadcast])
                           +- SortLimit(orderBy=[cr_refunded_cash ASC], offset=[0], fetch=[42], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[cr_refunded_cash ASC], offset=[0], fetch=[42], global=[false])
                                    +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o218070576.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#441439758:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[23](input=RelSubset#441439756,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[23]), rel#441439755:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[23](input=RelSubset#441439754,groupBy=cr_refunded_cdemo_sk,select=cr_refunded_cdemo_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (23) must be less than size (1)
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
    }
  }
}
"""



//Optimizer Branch Coverage: 0