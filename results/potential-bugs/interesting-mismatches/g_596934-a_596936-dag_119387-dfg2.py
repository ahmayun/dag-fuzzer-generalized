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

autonode_13 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_12 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_11 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_10 = autonode_13.alias('mJVwe')
autonode_9 = autonode_12.order_by(col('d_same_day_lq_node_12'))
autonode_8 = autonode_11.add_columns(lit("hello"))
autonode_7 = autonode_9.join(autonode_10, col('i_product_name_node_13') == col('d_current_quarter_node_12'))
autonode_6 = autonode_8.select(col('w_warehouse_sk_node_11'))
autonode_5 = autonode_7.group_by(col('d_current_month_node_12')).select(col('d_dom_node_12').count.alias('d_dom_node_12'))
autonode_4 = autonode_6.group_by(col('w_warehouse_sk_node_11')).select(col('w_warehouse_sk_node_11').count.alias('w_warehouse_sk_node_11'))
autonode_3 = autonode_4.join(autonode_5, col('d_dom_node_12') == col('w_warehouse_sk_node_11'))
autonode_2 = autonode_3.distinct()
autonode_1 = autonode_2.add_columns(lit("hello"))
sink = autonode_1.filter(col('w_warehouse_sk_node_11') > 19)
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
LogicalFilter(condition=[>($0, 19)])
+- LogicalProject(w_warehouse_sk_node_11=[$0], d_dom_node_12=[$1], _c2=[_UTF-16LE'hello'])
   +- LogicalAggregate(group=[{0, 1}])
      +- LogicalJoin(condition=[=($1, $0)], joinType=[inner])
         :- LogicalProject(w_warehouse_sk_node_11=[$1])
         :  +- LogicalAggregate(group=[{0}], EXPR$0=[COUNT($0)])
         :     +- LogicalProject(w_warehouse_sk_node_11=[$0])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])
         +- LogicalProject(d_dom_node_12=[$1])
            +- LogicalAggregate(group=[{25}], EXPR$0=[COUNT($9)])
               +- LogicalJoin(condition=[=($49, $26)], joinType=[inner])
                  :- LogicalSort(sort0=[$22], dir0=[ASC])
                  :  +- LogicalProject(d_date_sk_node_12=[$0], d_date_id_node_12=[$1], d_date_node_12=[$2], d_month_seq_node_12=[$3], d_week_seq_node_12=[$4], d_quarter_seq_node_12=[$5], d_year_node_12=[$6], d_dow_node_12=[$7], d_moy_node_12=[$8], d_dom_node_12=[$9], d_qoy_node_12=[$10], d_fy_year_node_12=[$11], d_fy_quarter_seq_node_12=[$12], d_fy_week_seq_node_12=[$13], d_day_name_node_12=[$14], d_quarter_name_node_12=[$15], d_holiday_node_12=[$16], d_weekend_node_12=[$17], d_following_holiday_node_12=[$18], d_first_dom_node_12=[$19], d_last_dom_node_12=[$20], d_same_day_ly_node_12=[$21], d_same_day_lq_node_12=[$22], d_current_day_node_12=[$23], d_current_week_node_12=[$24], d_current_month_node_12=[$25], d_current_quarter_node_12=[$26], d_current_year_node_12=[$27])
                  :     +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
                  +- LogicalProject(mJVwe=[AS($0, _UTF-16LE'mJVwe')], i_item_id_node_13=[$1], i_rec_start_date_node_13=[$2], i_rec_end_date_node_13=[$3], i_item_desc_node_13=[$4], i_current_price_node_13=[$5], i_wholesale_cost_node_13=[$6], i_brand_id_node_13=[$7], i_brand_node_13=[$8], i_class_id_node_13=[$9], i_class_node_13=[$10], i_category_id_node_13=[$11], i_category_node_13=[$12], i_manufact_id_node_13=[$13], i_manufact_node_13=[$14], i_size_node_13=[$15], i_formulation_node_13=[$16], i_color_node_13=[$17], i_units_node_13=[$18], i_container_node_13=[$19], i_manager_id_node_13=[$20], i_product_name_node_13=[$21])
                     +- LogicalTableScan(table=[[default_catalog, default_database, item]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS w_warehouse_sk_node_11, EXPR$00 AS d_dom_node_12, 'hello' AS _c2])
+- SortAggregate(isMerge=[false], groupBy=[EXPR$0, EXPR$00], select=[EXPR$0, EXPR$00])
   +- Sort(orderBy=[EXPR$0 ASC, EXPR$00 ASC])
      +- Exchange(distribution=[hash[EXPR$0, EXPR$00]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(EXPR$00, EXPR$0)], select=[EXPR$0, EXPR$00], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[EXPR$0], where=[>(EXPR$0, 19)])
            :     +- SortAggregate(isMerge=[true], groupBy=[w_warehouse_sk], select=[w_warehouse_sk, Final_COUNT(count$0) AS EXPR$0])
            :        +- Sort(orderBy=[w_warehouse_sk ASC])
            :           +- Exchange(distribution=[hash[w_warehouse_sk]])
            :              +- LocalSortAggregate(groupBy=[w_warehouse_sk], select=[w_warehouse_sk, Partial_COUNT(w_warehouse_sk) AS count$0])
            :                 +- Sort(orderBy=[w_warehouse_sk ASC])
            :                    +- TableSourceScan(table=[[default_catalog, default_database, warehouse, project=[w_warehouse_sk], metadata=[]]], fields=[w_warehouse_sk])
            +- Calc(select=[EXPR$0], where=[>(EXPR$0, 19)])
               +- HashAggregate(isMerge=[false], groupBy=[d_current_month], select=[d_current_month, COUNT(d_dom) AS EXPR$0])
                  +- Exchange(distribution=[hash[d_current_month]])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(i_product_name_node_13, d_current_quarter)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, mJVwe, i_item_id_node_13, i_rec_start_date_node_13, i_rec_end_date_node_13, i_item_desc_node_13, i_current_price_node_13, i_wholesale_cost_node_13, i_brand_id_node_13, i_brand_node_13, i_class_id_node_13, i_class_node_13, i_category_id_node_13, i_category_node_13, i_manufact_id_node_13, i_manufact_node_13, i_size_node_13, i_formulation_node_13, i_color_node_13, i_units_node_13, i_container_node_13, i_manager_id_node_13, i_product_name_node_13], build=[right])
                        :- Sort(orderBy=[d_same_day_lq ASC])
                        :  +- Exchange(distribution=[single])
                        :     +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[i_item_sk AS mJVwe, i_item_id AS i_item_id_node_13, i_rec_start_date AS i_rec_start_date_node_13, i_rec_end_date AS i_rec_end_date_node_13, i_item_desc AS i_item_desc_node_13, i_current_price AS i_current_price_node_13, i_wholesale_cost AS i_wholesale_cost_node_13, i_brand_id AS i_brand_id_node_13, i_brand AS i_brand_node_13, i_class_id AS i_class_id_node_13, i_class AS i_class_node_13, i_category_id AS i_category_id_node_13, i_category AS i_category_node_13, i_manufact_id AS i_manufact_id_node_13, i_manufact AS i_manufact_node_13, i_size AS i_size_node_13, i_formulation AS i_formulation_node_13, i_color AS i_color_node_13, i_units AS i_units_node_13, i_container AS i_container_node_13, i_manager_id AS i_manager_id_node_13, i_product_name AS i_product_name_node_13])
                              +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS w_warehouse_sk_node_11, EXPR$00 AS d_dom_node_12, 'hello' AS _c2])
+- SortAggregate(isMerge=[false], groupBy=[EXPR$0, EXPR$00], select=[EXPR$0, EXPR$00])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[EXPR$0 ASC, EXPR$00 ASC])
         +- Exchange(distribution=[hash[EXPR$0, EXPR$00]])
            +- MultipleInput(readOrder=[0,1], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(EXPR$00 = EXPR$0)], select=[EXPR$0, EXPR$00], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- Calc(select=[EXPR$0], where=[(EXPR$0 > 19)])\
   +- HashAggregate(isMerge=[false], groupBy=[d_current_month], select=[d_current_month, COUNT(d_dom) AS EXPR$0])\
      +- [#2] Exchange(distribution=[hash[d_current_month]])\
])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[EXPR$0], where=[(EXPR$0 > 19)])
               :     +- SortAggregate(isMerge=[true], groupBy=[w_warehouse_sk], select=[w_warehouse_sk, Final_COUNT(count$0) AS EXPR$0])
               :        +- Exchange(distribution=[forward])
               :           +- Sort(orderBy=[w_warehouse_sk ASC])
               :              +- Exchange(distribution=[hash[w_warehouse_sk]])
               :                 +- LocalSortAggregate(groupBy=[w_warehouse_sk], select=[w_warehouse_sk, Partial_COUNT(w_warehouse_sk) AS count$0])
               :                    +- Exchange(distribution=[forward])
               :                       +- Sort(orderBy=[w_warehouse_sk ASC])
               :                          +- TableSourceScan(table=[[default_catalog, default_database, warehouse, project=[w_warehouse_sk], metadata=[]]], fields=[w_warehouse_sk])
               +- Exchange(distribution=[hash[d_current_month]])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[(i_product_name_node_13 = d_current_quarter)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, mJVwe, i_item_id_node_13, i_rec_start_date_node_13, i_rec_end_date_node_13, i_item_desc_node_13, i_current_price_node_13, i_wholesale_cost_node_13, i_brand_id_node_13, i_brand_node_13, i_class_id_node_13, i_class_node_13, i_category_id_node_13, i_category_node_13, i_manufact_id_node_13, i_manufact_node_13, i_size_node_13, i_formulation_node_13, i_color_node_13, i_units_node_13, i_container_node_13, i_manager_id_node_13, i_product_name_node_13], build=[right])
                     :- Sort(orderBy=[d_same_day_lq ASC])
                     :  +- Exchange(distribution=[single])
                     :     +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
                     +- Exchange(distribution=[broadcast])
                        +- Calc(select=[i_item_sk AS mJVwe, i_item_id AS i_item_id_node_13, i_rec_start_date AS i_rec_start_date_node_13, i_rec_end_date AS i_rec_end_date_node_13, i_item_desc AS i_item_desc_node_13, i_current_price AS i_current_price_node_13, i_wholesale_cost AS i_wholesale_cost_node_13, i_brand_id AS i_brand_id_node_13, i_brand AS i_brand_node_13, i_class_id AS i_class_id_node_13, i_class AS i_class_node_13, i_category_id AS i_category_id_node_13, i_category AS i_category_node_13, i_manufact_id AS i_manufact_id_node_13, i_manufact AS i_manufact_node_13, i_size AS i_size_node_13, i_formulation AS i_formulation_node_13, i_color AS i_color_node_13, i_units AS i_units_node_13, i_container AS i_container_node_13, i_manager_id AS i_manager_id_node_13, i_product_name AS i_product_name_node_13])
                           +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o325573084.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#656702408:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[22](input=RelSubset#656702406,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[22]), rel#656702405:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[22](input=RelSubset#656702404,groupBy=d_current_month, d_current_quarter,select=d_current_month, d_current_quarter, Partial_COUNT(d_dom) AS count$0)]
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