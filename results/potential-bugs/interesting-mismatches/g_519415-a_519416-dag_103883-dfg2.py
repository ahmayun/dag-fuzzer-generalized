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

autonode_10 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_9 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_8 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_7 = autonode_10.distinct()
autonode_6 = autonode_9.order_by(col('cp_description_node_9'))
autonode_5 = autonode_8.group_by(col('cr_ship_mode_sk_node_8')).select(col('cr_refunded_addr_sk_node_8').sum.alias('cr_refunded_addr_sk_node_8'))
autonode_4 = autonode_7.add_columns(lit("hello"))
autonode_3 = autonode_5.join(autonode_6, col('cr_refunded_addr_sk_node_8') == col('cp_end_date_sk_node_9'))
autonode_2 = autonode_3.join(autonode_4, col('sm_ship_mode_id_node_10') == col('cp_type_node_9'))
autonode_1 = autonode_2.group_by(col('cp_catalog_page_number_node_9')).select(col('sm_ship_mode_id_node_10').min.alias('sm_ship_mode_id_node_10'))
sink = autonode_1.limit(97)
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
LogicalSort(fetch=[97])
+- LogicalProject(sm_ship_mode_id_node_10=[$1])
   +- LogicalAggregate(group=[{7}], EXPR$0=[MIN($11)])
      +- LogicalJoin(condition=[=($11, $9)], joinType=[inner])
         :- LogicalJoin(condition=[=($0, $4)], joinType=[inner])
         :  :- LogicalProject(cr_refunded_addr_sk_node_8=[$1])
         :  :  +- LogicalAggregate(group=[{1}], EXPR$0=[SUM($0)])
         :  :     +- LogicalProject(cr_refunded_addr_sk_node_8=[$6], cr_ship_mode_sk_node_8=[$13])
         :  :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
         :  +- LogicalSort(sort0=[$7], dir0=[ASC])
         :     +- LogicalProject(cp_catalog_page_sk_node_9=[$0], cp_catalog_page_id_node_9=[$1], cp_start_date_sk_node_9=[$2], cp_end_date_sk_node_9=[$3], cp_department_node_9=[$4], cp_catalog_number_node_9=[$5], cp_catalog_page_number_node_9=[$6], cp_description_node_9=[$7], cp_type_node_9=[$8])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])
         +- LogicalProject(sm_ship_mode_sk_node_10=[$0], sm_ship_mode_id_node_10=[$1], sm_type_node_10=[$2], sm_code_node_10=[$3], sm_carrier_node_10=[$4], sm_contract_node_10=[$5], _c6=[_UTF-16LE'hello'])
            +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5}])
               +- LogicalProject(sm_ship_mode_sk_node_10=[$0], sm_ship_mode_id_node_10=[$1], sm_type_node_10=[$2], sm_code_node_10=[$3], sm_carrier_node_10=[$4], sm_contract_node_10=[$5])
                  +- LogicalTableScan(table=[[default_catalog, default_database, ship_mode]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[97], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[97], global=[false])
      +- Calc(select=[EXPR$0 AS sm_ship_mode_id_node_10])
         +- SortAggregate(isMerge=[true], groupBy=[cp_catalog_page_number], select=[cp_catalog_page_number, Final_MIN(min$0) AS EXPR$0])
            +- Sort(orderBy=[cp_catalog_page_number ASC])
               +- Exchange(distribution=[hash[cp_catalog_page_number]])
                  +- LocalSortAggregate(groupBy=[cp_catalog_page_number], select=[cp_catalog_page_number, Partial_MIN(sm_ship_mode_id_node_10) AS min$0])
                     +- Sort(orderBy=[cp_catalog_page_number ASC])
                        +- HashJoin(joinType=[InnerJoin], where=[=(sm_ship_mode_id_node_10, cp_type)], select=[cr_refunded_addr_sk_node_8, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, sm_ship_mode_sk_node_10, sm_ship_mode_id_node_10, sm_type_node_10, sm_code_node_10, sm_carrier_node_10, sm_contract_node_10, _c6], isBroadcast=[true], build=[right])
                           :- HashJoin(joinType=[InnerJoin], where=[=(cr_refunded_addr_sk_node_8, cp_end_date_sk)], select=[cr_refunded_addr_sk_node_8, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], isBroadcast=[true], build=[left])
                           :  :- Exchange(distribution=[broadcast])
                           :  :  +- Calc(select=[EXPR$0 AS cr_refunded_addr_sk_node_8])
                           :  :     +- HashAggregate(isMerge=[true], groupBy=[cr_ship_mode_sk], select=[cr_ship_mode_sk, Final_SUM(sum$0) AS EXPR$0])
                           :  :        +- Exchange(distribution=[hash[cr_ship_mode_sk]])
                           :  :           +- LocalHashAggregate(groupBy=[cr_ship_mode_sk], select=[cr_ship_mode_sk, Partial_SUM(cr_refunded_addr_sk) AS sum$0])
                           :  :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_refunded_addr_sk, cr_ship_mode_sk], metadata=[]]], fields=[cr_refunded_addr_sk, cr_ship_mode_sk])
                           :  +- Sort(orderBy=[cp_description ASC])
                           :     +- Exchange(distribution=[single])
                           :        +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
                           +- Exchange(distribution=[broadcast])
                              +- Calc(select=[sm_ship_mode_sk AS sm_ship_mode_sk_node_10, sm_ship_mode_id AS sm_ship_mode_id_node_10, sm_type AS sm_type_node_10, sm_code AS sm_code_node_10, sm_carrier AS sm_carrier_node_10, sm_contract AS sm_contract_node_10, 'hello' AS _c6])
                                 +- HashAggregate(isMerge=[true], groupBy=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
                                    +- Exchange(distribution=[hash[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract]])
                                       +- LocalHashAggregate(groupBy=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
                                          +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[97], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[97], global=[false])
      +- Calc(select=[EXPR$0 AS sm_ship_mode_id_node_10])
         +- SortAggregate(isMerge=[true], groupBy=[cp_catalog_page_number], select=[cp_catalog_page_number, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[forward])
               +- Sort(orderBy=[cp_catalog_page_number ASC])
                  +- Exchange(distribution=[hash[cp_catalog_page_number]])
                     +- LocalSortAggregate(groupBy=[cp_catalog_page_number], select=[cp_catalog_page_number, Partial_MIN(sm_ship_mode_id_node_10) AS min$0])
                        +- Exchange(distribution=[forward])
                           +- Sort(orderBy=[cp_catalog_page_number ASC])
                              +- MultipleInput(readOrder=[0,0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(sm_ship_mode_id_node_10 = cp_type)], select=[cr_refunded_addr_sk_node_8, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, sm_ship_mode_sk_node_10, sm_ship_mode_id_node_10, sm_type_node_10, sm_code_node_10, sm_carrier_node_10, sm_contract_node_10, _c6], isBroadcast=[true], build=[right])\
:- HashJoin(joinType=[InnerJoin], where=[(cr_refunded_addr_sk_node_8 = cp_end_date_sk)], select=[cr_refunded_addr_sk_node_8, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], isBroadcast=[true], build=[left])\
:  :- [#2] Exchange(distribution=[broadcast])\
:  +- [#3] Sort(orderBy=[cp_description ASC])\
+- [#1] Exchange(distribution=[broadcast])\
])
                                 :- Exchange(distribution=[broadcast])
                                 :  +- Calc(select=[sm_ship_mode_sk AS sm_ship_mode_sk_node_10, sm_ship_mode_id AS sm_ship_mode_id_node_10, sm_type AS sm_type_node_10, sm_code AS sm_code_node_10, sm_carrier AS sm_carrier_node_10, sm_contract AS sm_contract_node_10, 'hello' AS _c6])
                                 :     +- HashAggregate(isMerge=[true], groupBy=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
                                 :        +- Exchange(distribution=[hash[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract]])
                                 :           +- LocalHashAggregate(groupBy=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
                                 :              +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
                                 :- Exchange(distribution=[broadcast])
                                 :  +- Calc(select=[EXPR$0 AS cr_refunded_addr_sk_node_8])
                                 :     +- HashAggregate(isMerge=[true], groupBy=[cr_ship_mode_sk], select=[cr_ship_mode_sk, Final_SUM(sum$0) AS EXPR$0])
                                 :        +- Exchange(distribution=[hash[cr_ship_mode_sk]])
                                 :           +- LocalHashAggregate(groupBy=[cr_ship_mode_sk], select=[cr_ship_mode_sk, Partial_SUM(cr_refunded_addr_sk) AS sum$0])
                                 :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_refunded_addr_sk, cr_ship_mode_sk], metadata=[]]], fields=[cr_refunded_addr_sk, cr_ship_mode_sk])
                                 +- Sort(orderBy=[cp_description ASC])
                                    +- Exchange(distribution=[single])
                                       +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o282991891.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#572043838:AbstractConverter.BATCH_PHYSICAL.hash[0, 1, 2]true.[7](input=RelSubset#572043836,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1, 2]true,sort=[7]), rel#572043835:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[7](input=RelSubset#572043834,groupBy=cp_end_date_sk, cp_catalog_page_number, cp_type,select=cp_end_date_sk, cp_catalog_page_number, cp_type)]
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