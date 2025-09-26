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
    return values.mean()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_9 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_8 = autonode_10.alias('cZVso')
autonode_7 = autonode_9.group_by(col('ws_sales_price_node_9')).select(col('ws_bill_hdemo_sk_node_9').max.alias('ws_bill_hdemo_sk_node_9'))
autonode_6 = autonode_8.order_by(col('cc_street_type_node_10'))
autonode_5 = autonode_7.limit(42)
autonode_4 = autonode_6.alias('OZDys')
autonode_3 = autonode_5.select(col('ws_bill_hdemo_sk_node_9'))
autonode_2 = autonode_3.join(autonode_4, col('cc_employees_node_10') == col('ws_bill_hdemo_sk_node_9'))
autonode_1 = autonode_2.group_by(col('cc_market_manager_node_10')).select(col('cc_mkt_id_node_10').min.alias('cc_mkt_id_node_10'))
sink = autonode_1.limit(2)
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
LogicalSort(fetch=[2])
+- LogicalProject(cc_mkt_id_node_10=[$1])
   +- LogicalAggregate(group=[{16}], EXPR$0=[MIN($13)])
      +- LogicalJoin(condition=[=($9, $0)], joinType=[inner])
         :- LogicalProject(ws_bill_hdemo_sk_node_9=[$0])
         :  +- LogicalSort(fetch=[42])
         :     +- LogicalProject(ws_bill_hdemo_sk_node_9=[$1])
         :        +- LogicalAggregate(group=[{1}], EXPR$0=[MAX($0)])
         :           +- LogicalProject(ws_bill_hdemo_sk_node_9=[$6], ws_sales_price_node_9=[$21])
         :              +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
         +- LogicalProject(OZDys=[AS($0, _UTF-16LE'OZDys')], cc_call_center_id_node_10=[$1], cc_rec_start_date_node_10=[$2], cc_rec_end_date_node_10=[$3], cc_closed_date_sk_node_10=[$4], cc_open_date_sk_node_10=[$5], cc_name_node_10=[$6], cc_class_node_10=[$7], cc_employees_node_10=[$8], cc_sq_ft_node_10=[$9], cc_hours_node_10=[$10], cc_manager_node_10=[$11], cc_mkt_id_node_10=[$12], cc_mkt_class_node_10=[$13], cc_mkt_desc_node_10=[$14], cc_market_manager_node_10=[$15], cc_division_node_10=[$16], cc_division_name_node_10=[$17], cc_company_node_10=[$18], cc_company_name_node_10=[$19], cc_street_number_node_10=[$20], cc_street_name_node_10=[$21], cc_street_type_node_10=[$22], cc_suite_number_node_10=[$23], cc_city_node_10=[$24], cc_county_node_10=[$25], cc_state_node_10=[$26], cc_zip_node_10=[$27], cc_country_node_10=[$28], cc_gmt_offset_node_10=[$29], cc_tax_percentage_node_10=[$30])
            +- LogicalSort(sort0=[$22], dir0=[ASC])
               +- LogicalProject(cZVso=[AS($0, _UTF-16LE'cZVso')], cc_call_center_id_node_10=[$1], cc_rec_start_date_node_10=[$2], cc_rec_end_date_node_10=[$3], cc_closed_date_sk_node_10=[$4], cc_open_date_sk_node_10=[$5], cc_name_node_10=[$6], cc_class_node_10=[$7], cc_employees_node_10=[$8], cc_sq_ft_node_10=[$9], cc_hours_node_10=[$10], cc_manager_node_10=[$11], cc_mkt_id_node_10=[$12], cc_mkt_class_node_10=[$13], cc_mkt_desc_node_10=[$14], cc_market_manager_node_10=[$15], cc_division_node_10=[$16], cc_division_name_node_10=[$17], cc_company_node_10=[$18], cc_company_name_node_10=[$19], cc_street_number_node_10=[$20], cc_street_name_node_10=[$21], cc_street_type_node_10=[$22], cc_suite_number_node_10=[$23], cc_city_node_10=[$24], cc_county_node_10=[$25], cc_state_node_10=[$26], cc_zip_node_10=[$27], cc_country_node_10=[$28], cc_gmt_offset_node_10=[$29], cc_tax_percentage_node_10=[$30])
                  +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[2], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[2], global=[false])
      +- Calc(select=[EXPR$0 AS cc_mkt_id_node_10])
         +- SortAggregate(isMerge=[true], groupBy=[cc_market_manager_node_10], select=[cc_market_manager_node_10, Final_MIN(min$0) AS EXPR$0])
            +- Sort(orderBy=[cc_market_manager_node_10 ASC])
               +- Exchange(distribution=[hash[cc_market_manager_node_10]])
                  +- LocalSortAggregate(groupBy=[cc_market_manager_node_10], select=[cc_market_manager_node_10, Partial_MIN(cc_mkt_id_node_10) AS min$0])
                     +- Sort(orderBy=[cc_market_manager_node_10 ASC])
                        +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cc_employees_node_10, ws_bill_hdemo_sk_node_9)], select=[ws_bill_hdemo_sk_node_9, OZDys, cc_call_center_id_node_10, cc_rec_start_date_node_10, cc_rec_end_date_node_10, cc_closed_date_sk_node_10, cc_open_date_sk_node_10, cc_name_node_10, cc_class_node_10, cc_employees_node_10, cc_sq_ft_node_10, cc_hours_node_10, cc_manager_node_10, cc_mkt_id_node_10, cc_mkt_class_node_10, cc_mkt_desc_node_10, cc_market_manager_node_10, cc_division_node_10, cc_division_name_node_10, cc_company_node_10, cc_company_name_node_10, cc_street_number_node_10, cc_street_name_node_10, cc_street_type_node_10, cc_suite_number_node_10, cc_city_node_10, cc_county_node_10, cc_state_node_10, cc_zip_node_10, cc_country_node_10, cc_gmt_offset_node_10, cc_tax_percentage_node_10], build=[left])
                           :- Exchange(distribution=[broadcast])
                           :  +- Calc(select=[EXPR$0 AS ws_bill_hdemo_sk_node_9])
                           :     +- Limit(offset=[0], fetch=[42], global=[true])
                           :        +- Exchange(distribution=[single])
                           :           +- Limit(offset=[0], fetch=[42], global=[false])
                           :              +- HashAggregate(isMerge=[true], groupBy=[ws_sales_price], select=[ws_sales_price, Final_MAX(max$0) AS EXPR$0])
                           :                 +- Exchange(distribution=[hash[ws_sales_price]])
                           :                    +- LocalHashAggregate(groupBy=[ws_sales_price], select=[ws_sales_price, Partial_MAX(ws_bill_hdemo_sk) AS max$0])
                           :                       +- TableSourceScan(table=[[default_catalog, default_database, web_sales, project=[ws_bill_hdemo_sk, ws_sales_price], metadata=[]]], fields=[ws_bill_hdemo_sk, ws_sales_price])
                           +- Calc(select=[cc_call_center_sk AS OZDys, cc_call_center_id AS cc_call_center_id_node_10, cc_rec_start_date AS cc_rec_start_date_node_10, cc_rec_end_date AS cc_rec_end_date_node_10, cc_closed_date_sk AS cc_closed_date_sk_node_10, cc_open_date_sk AS cc_open_date_sk_node_10, cc_name AS cc_name_node_10, cc_class AS cc_class_node_10, cc_employees AS cc_employees_node_10, cc_sq_ft AS cc_sq_ft_node_10, cc_hours AS cc_hours_node_10, cc_manager AS cc_manager_node_10, cc_mkt_id AS cc_mkt_id_node_10, cc_mkt_class AS cc_mkt_class_node_10, cc_mkt_desc AS cc_mkt_desc_node_10, cc_market_manager AS cc_market_manager_node_10, cc_division AS cc_division_node_10, cc_division_name AS cc_division_name_node_10, cc_company AS cc_company_node_10, cc_company_name AS cc_company_name_node_10, cc_street_number AS cc_street_number_node_10, cc_street_name AS cc_street_name_node_10, cc_street_type AS cc_street_type_node_10, cc_suite_number AS cc_suite_number_node_10, cc_city AS cc_city_node_10, cc_county AS cc_county_node_10, cc_state AS cc_state_node_10, cc_zip AS cc_zip_node_10, cc_country AS cc_country_node_10, cc_gmt_offset AS cc_gmt_offset_node_10, cc_tax_percentage AS cc_tax_percentage_node_10])
                              +- Sort(orderBy=[cc_street_type ASC])
                                 +- Exchange(distribution=[single])
                                    +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[2], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[2], global=[false])
      +- Calc(select=[EXPR$0 AS cc_mkt_id_node_10])
         +- SortAggregate(isMerge=[true], groupBy=[cc_market_manager_node_10], select=[cc_market_manager_node_10, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[forward])
               +- Sort(orderBy=[cc_market_manager_node_10 ASC])
                  +- Exchange(distribution=[hash[cc_market_manager_node_10]])
                     +- LocalSortAggregate(groupBy=[cc_market_manager_node_10], select=[cc_market_manager_node_10, Partial_MIN(cc_mkt_id_node_10) AS min$0])
                        +- Exchange(distribution=[forward])
                           +- Sort(orderBy=[cc_market_manager_node_10 ASC])
                              +- NestedLoopJoin(joinType=[InnerJoin], where=[(cc_employees_node_10 = ws_bill_hdemo_sk_node_9)], select=[ws_bill_hdemo_sk_node_9, OZDys, cc_call_center_id_node_10, cc_rec_start_date_node_10, cc_rec_end_date_node_10, cc_closed_date_sk_node_10, cc_open_date_sk_node_10, cc_name_node_10, cc_class_node_10, cc_employees_node_10, cc_sq_ft_node_10, cc_hours_node_10, cc_manager_node_10, cc_mkt_id_node_10, cc_mkt_class_node_10, cc_mkt_desc_node_10, cc_market_manager_node_10, cc_division_node_10, cc_division_name_node_10, cc_company_node_10, cc_company_name_node_10, cc_street_number_node_10, cc_street_name_node_10, cc_street_type_node_10, cc_suite_number_node_10, cc_city_node_10, cc_county_node_10, cc_state_node_10, cc_zip_node_10, cc_country_node_10, cc_gmt_offset_node_10, cc_tax_percentage_node_10], build=[left])
                                 :- Exchange(distribution=[broadcast])
                                 :  +- Calc(select=[EXPR$0 AS ws_bill_hdemo_sk_node_9])
                                 :     +- Limit(offset=[0], fetch=[42], global=[true])
                                 :        +- Exchange(distribution=[single])
                                 :           +- Limit(offset=[0], fetch=[42], global=[false])
                                 :              +- HashAggregate(isMerge=[true], groupBy=[ws_sales_price], select=[ws_sales_price, Final_MAX(max$0) AS EXPR$0])
                                 :                 +- Exchange(distribution=[hash[ws_sales_price]])
                                 :                    +- LocalHashAggregate(groupBy=[ws_sales_price], select=[ws_sales_price, Partial_MAX(ws_bill_hdemo_sk) AS max$0])
                                 :                       +- TableSourceScan(table=[[default_catalog, default_database, web_sales, project=[ws_bill_hdemo_sk, ws_sales_price], metadata=[]]], fields=[ws_bill_hdemo_sk, ws_sales_price])
                                 +- Calc(select=[cc_call_center_sk AS OZDys, cc_call_center_id AS cc_call_center_id_node_10, cc_rec_start_date AS cc_rec_start_date_node_10, cc_rec_end_date AS cc_rec_end_date_node_10, cc_closed_date_sk AS cc_closed_date_sk_node_10, cc_open_date_sk AS cc_open_date_sk_node_10, cc_name AS cc_name_node_10, cc_class AS cc_class_node_10, cc_employees AS cc_employees_node_10, cc_sq_ft AS cc_sq_ft_node_10, cc_hours AS cc_hours_node_10, cc_manager AS cc_manager_node_10, cc_mkt_id AS cc_mkt_id_node_10, cc_mkt_class AS cc_mkt_class_node_10, cc_mkt_desc AS cc_mkt_desc_node_10, cc_market_manager AS cc_market_manager_node_10, cc_division AS cc_division_node_10, cc_division_name AS cc_division_name_node_10, cc_company AS cc_company_node_10, cc_company_name AS cc_company_name_node_10, cc_street_number AS cc_street_number_node_10, cc_street_name AS cc_street_name_node_10, cc_street_type AS cc_street_type_node_10, cc_suite_number AS cc_suite_number_node_10, cc_city AS cc_city_node_10, cc_county AS cc_county_node_10, cc_state AS cc_state_node_10, cc_zip AS cc_zip_node_10, cc_country AS cc_country_node_10, cc_gmt_offset AS cc_gmt_offset_node_10, cc_tax_percentage AS cc_tax_percentage_node_10])
                                    +- Sort(orderBy=[cc_street_type ASC])
                                       +- Exchange(distribution=[single])
                                          +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o2136848.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#4011582:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[22](input=RelSubset#4011580,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[22]), rel#4011579:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[22](input=RelSubset#4011578,groupBy=cc_employees, cc_market_manager,select=cc_employees, cc_market_manager, Partial_MIN(cc_mkt_id) AS min$0)]
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