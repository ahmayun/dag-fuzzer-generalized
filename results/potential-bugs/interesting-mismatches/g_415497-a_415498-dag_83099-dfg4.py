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
    return values.var()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_9 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_8 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_7 = autonode_9.add_columns(lit("hello"))
autonode_6 = autonode_8.order_by(col('i_color_node_8'))
autonode_5 = autonode_6.join(autonode_7, col('cp_department_node_9') == col('i_size_node_8'))
autonode_4 = autonode_5.group_by(col('i_item_id_node_8')).select(col('i_current_price_node_8').min.alias('i_current_price_node_8'))
autonode_3 = autonode_4.group_by(col('i_current_price_node_8')).select(col('i_current_price_node_8').min.alias('i_current_price_node_8'))
autonode_2 = autonode_3.limit(54)
autonode_1 = autonode_2.add_columns(lit("hello"))
sink = autonode_1.filter(col('i_current_price_node_8') <= -42.153578996658325)
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
LogicalFilter(condition=[<=($0, -4.2153578996658325E1:DOUBLE)])
+- LogicalProject(i_current_price_node_8=[$0], _c1=[_UTF-16LE'hello'])
   +- LogicalSort(fetch=[54])
      +- LogicalProject(i_current_price_node_8=[$1])
         +- LogicalAggregate(group=[{0}], EXPR$0=[MIN($0)])
            +- LogicalProject(i_current_price_node_8=[$1])
               +- LogicalAggregate(group=[{1}], EXPR$0=[MIN($5)])
                  +- LogicalJoin(condition=[=($26, $15)], joinType=[inner])
                     :- LogicalSort(sort0=[$17], dir0=[ASC])
                     :  +- LogicalProject(i_item_sk_node_8=[$0], i_item_id_node_8=[$1], i_rec_start_date_node_8=[$2], i_rec_end_date_node_8=[$3], i_item_desc_node_8=[$4], i_current_price_node_8=[$5], i_wholesale_cost_node_8=[$6], i_brand_id_node_8=[$7], i_brand_node_8=[$8], i_class_id_node_8=[$9], i_class_node_8=[$10], i_category_id_node_8=[$11], i_category_node_8=[$12], i_manufact_id_node_8=[$13], i_manufact_node_8=[$14], i_size_node_8=[$15], i_formulation_node_8=[$16], i_color_node_8=[$17], i_units_node_8=[$18], i_container_node_8=[$19], i_manager_id_node_8=[$20], i_product_name_node_8=[$21])
                     :     +- LogicalTableScan(table=[[default_catalog, default_database, item]])
                     +- LogicalProject(cp_catalog_page_sk_node_9=[$0], cp_catalog_page_id_node_9=[$1], cp_start_date_sk_node_9=[$2], cp_end_date_sk_node_9=[$3], cp_department_node_9=[$4], cp_catalog_number_node_9=[$5], cp_catalog_page_number_node_9=[$6], cp_description_node_9=[$7], cp_type_node_9=[$8], _c9=[_UTF-16LE'hello'])
                        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS i_current_price_node_8, 'hello' AS _c1], where=[<=(EXPR$0, -4.2153578996658325E1)])
+- Limit(offset=[0], fetch=[54], global=[true])
   +- Exchange(distribution=[single])
      +- Limit(offset=[0], fetch=[54], global=[false])
         +- HashAggregate(isMerge=[true], groupBy=[i_current_price_node_8], select=[i_current_price_node_8, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[hash[i_current_price_node_8]])
               +- LocalHashAggregate(groupBy=[i_current_price_node_8], select=[i_current_price_node_8, Partial_MIN(i_current_price_node_8) AS min$0])
                  +- Calc(select=[EXPR$0 AS i_current_price_node_8])
                     +- HashAggregate(isMerge=[true], groupBy=[i_item_id], select=[i_item_id, Final_MIN(min$0) AS EXPR$0])
                        +- Exchange(distribution=[hash[i_item_id]])
                           +- LocalHashAggregate(groupBy=[i_item_id], select=[i_item_id, Partial_MIN(i_current_price) AS min$0])
                              +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cp_department_node_9, i_size)], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name, cp_catalog_page_sk_node_9, cp_catalog_page_id_node_9, cp_start_date_sk_node_9, cp_end_date_sk_node_9, cp_department_node_9, cp_catalog_number_node_9, cp_catalog_page_number_node_9, cp_description_node_9, cp_type_node_9, _c9], build=[right])
                                 :- Sort(orderBy=[i_color ASC])
                                 :  +- Exchange(distribution=[single])
                                 :     +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
                                 +- Exchange(distribution=[broadcast])
                                    +- Calc(select=[cp_catalog_page_sk AS cp_catalog_page_sk_node_9, cp_catalog_page_id AS cp_catalog_page_id_node_9, cp_start_date_sk AS cp_start_date_sk_node_9, cp_end_date_sk AS cp_end_date_sk_node_9, cp_department AS cp_department_node_9, cp_catalog_number AS cp_catalog_number_node_9, cp_catalog_page_number AS cp_catalog_page_number_node_9, cp_description AS cp_description_node_9, cp_type AS cp_type_node_9, 'hello' AS _c9])
                                       +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS i_current_price_node_8, 'hello' AS _c1], where=[(EXPR$0 <= -4.2153578996658325E1)])
+- Limit(offset=[0], fetch=[54], global=[true])
   +- Exchange(distribution=[single])
      +- Limit(offset=[0], fetch=[54], global=[false])
         +- HashAggregate(isMerge=[true], groupBy=[i_current_price_node_8], select=[i_current_price_node_8, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[hash[i_current_price_node_8]])
               +- LocalHashAggregate(groupBy=[i_current_price_node_8], select=[i_current_price_node_8, Partial_MIN(i_current_price_node_8) AS min$0])
                  +- Calc(select=[EXPR$0 AS i_current_price_node_8])
                     +- HashAggregate(isMerge=[true], groupBy=[i_item_id], select=[i_item_id, Final_MIN(min$0) AS EXPR$0])
                        +- Exchange(distribution=[hash[i_item_id]])
                           +- LocalHashAggregate(groupBy=[i_item_id], select=[i_item_id, Partial_MIN(i_current_price) AS min$0])
                              +- NestedLoopJoin(joinType=[InnerJoin], where=[(cp_department_node_9 = i_size)], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name, cp_catalog_page_sk_node_9, cp_catalog_page_id_node_9, cp_start_date_sk_node_9, cp_end_date_sk_node_9, cp_department_node_9, cp_catalog_number_node_9, cp_catalog_page_number_node_9, cp_description_node_9, cp_type_node_9, _c9], build=[right])
                                 :- Sort(orderBy=[i_color ASC])
                                 :  +- Exchange(distribution=[single])
                                 :     +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
                                 +- Exchange(distribution=[broadcast])
                                    +- Calc(select=[cp_catalog_page_sk AS cp_catalog_page_sk_node_9, cp_catalog_page_id AS cp_catalog_page_id_node_9, cp_start_date_sk AS cp_start_date_sk_node_9, cp_end_date_sk AS cp_end_date_sk_node_9, cp_department AS cp_department_node_9, cp_catalog_number AS cp_catalog_number_node_9, cp_catalog_page_number AS cp_catalog_page_number_node_9, cp_description AS cp_description_node_9, cp_type AS cp_type_node_9, 'hello' AS _c9])
                                       +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o226120114.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#457870990:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[17](input=RelSubset#457870988,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[17]), rel#457870987:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[17](input=RelSubset#457870986,groupBy=i_item_id, i_size,select=i_item_id, i_size, Partial_MIN(i_current_price) AS min$0)]
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