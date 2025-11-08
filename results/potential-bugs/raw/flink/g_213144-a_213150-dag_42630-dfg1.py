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
def filter_udf_string(arg):
    return arg.startswith('A') or arg.startswith('a')
@udf(result_type=DataTypes.BOOLEAN())
def filter_udf_integer(arg):
    return str(arg)[::-1] == str(arg)
@udf(result_type=DataTypes.BOOLEAN())
def filter_udf_decimal(arg):
    return arg > 0.0 and arg < 1000.0
def preloaded_aggregation(values: pd.Series) -> float:
    return values.std()


env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass



table_env.execute_sql(f"""
        CREATE TEMPORARY TABLE catalog_page (
            cp_catalog_page_sk INT,
            cp_catalog_page_id STRING,
            cp_start_date_sk INT,
            cp_end_date_sk INT,
            cp_department STRING,
            cp_catalog_number INT,
            cp_catalog_page_number INT,
            cp_description STRING,
            cp_type STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'tpcds-csv/catalog_page',
            'format' = 'csv',
            'csv.field-delimiter' = ',',
            'csv.ignore-first-line' = 'true',
            'csv.allow-comments' = 'false',
            'csv.ignore-parse-errors' = 'true'
        )
""")
preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")
table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)
autonode_5 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_4 = autonode_5.limit(66)
autonode_3 = autonode_4.distinct()
autonode_2 = autonode_3.order_by(col('cp_department_node_5'))
autonode_1 = autonode_2.distinct()
sink = autonode_1.limit(28)
print(sink.explain())

# ======== Details ========
"""
{
  "is_same": true,
  "result_name": "NullPointerException",
  "result_details": {
    "opt": {
      "success": false,
      "error_name": "NullPointerException",
      "error_message": "An error occurred while calling o116366755.explain.
: java.lang.NullPointerException: metadataHandlerProvider
\tat java.base/java.util.Objects.requireNonNull(Objects.java:248)
\tat org.apache.calcite.rel.metadata.RelMetadataQueryBase.getMetadataHandlerProvider(RelMetadataQueryBase.java:122)
\tat org.apache.calcite.rel.metadata.RelMetadataQueryBase.revise(RelMetadataQueryBase.java:118)
\tat org.apache.calcite.rel.metadata.RelMetadataQuery.areColumnsUnique(RelMetadataQuery.java:585)
\tat org.apache.calcite.rel.metadata.RelMetadataQuery.areColumnsUnique(RelMetadataQuery.java:562)
\tat org.apache.flink.table.planner.plan.rules.logical.FlinkAggregateRemoveRule.matches(FlinkAggregateRemoveRule.java:93)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.matchRecurse(VolcanoRuleCall.java:278)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.matchRecurse(VolcanoRuleCall.java:410)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.match(VolcanoRuleCall.java:262)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.fireRules(VolcanoPlanner.java:1090)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.registerImpl(VolcanoPlanner.java:1386)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.register(VolcanoPlanner.java:598)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:613)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:95)
\tat org.apache.calcite.rel.AbstractRelNode.onRegister(AbstractRelNode.java:273)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.registerImpl(VolcanoPlanner.java:1271)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.register(VolcanoPlanner.java:598)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:613)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.changeTraits(VolcanoPlanner.java:498)
\tat org.apache.calcite.tools.Programs$RuleSetProgram.run(Programs.java:314)
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
\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
\tat java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
\tat java.base/java.lang.reflect.Method.invoke(Method.java:566)
\tat org.apache.flink.api.python.shaded.py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
\tat org.apache.flink.api.python.shaded.py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
\tat org.apache.flink.api.python.shaded.py4j.Gateway.invoke(Gateway.java:282)
\tat org.apache.flink.api.python.shaded.py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
\tat org.apache.flink.api.python.shaded.py4j.commands.CallCommand.execute(CallCommand.java:79)
\tat org.apache.flink.api.python.shaded.py4j.GatewayConnection.run(GatewayConnection.java:238)
\tat java.base/java.lang.Thread.run(Thread.java:829)
",
      "stdout": "",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "NullPointerException",
      "error_message": "An error occurred while calling o116366811.explain.
: java.lang.NullPointerException: metadataHandlerProvider
\tat java.base/java.util.Objects.requireNonNull(Objects.java:248)
\tat org.apache.calcite.rel.metadata.RelMetadataQueryBase.getMetadataHandlerProvider(RelMetadataQueryBase.java:122)
\tat org.apache.calcite.rel.metadata.RelMetadataQueryBase.revise(RelMetadataQueryBase.java:118)
\tat org.apache.calcite.rel.metadata.RelMetadataQuery.areColumnsUnique(RelMetadataQuery.java:585)
\tat org.apache.calcite.rel.metadata.RelMetadataQuery.areColumnsUnique(RelMetadataQuery.java:562)
\tat org.apache.flink.table.planner.plan.rules.logical.FlinkAggregateRemoveRule.matches(FlinkAggregateRemoveRule.java:93)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.matchRecurse(VolcanoRuleCall.java:278)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.matchRecurse(VolcanoRuleCall.java:410)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.match(VolcanoRuleCall.java:262)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.fireRules(VolcanoPlanner.java:1090)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.registerImpl(VolcanoPlanner.java:1386)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.register(VolcanoPlanner.java:598)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:613)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:95)
\tat org.apache.calcite.rel.AbstractRelNode.onRegister(AbstractRelNode.java:273)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.registerImpl(VolcanoPlanner.java:1271)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.register(VolcanoPlanner.java:598)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:613)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.changeTraits(VolcanoPlanner.java:498)
\tat org.apache.calcite.tools.Programs$RuleSetProgram.run(Programs.java:314)
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
\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
\tat java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
\tat java.base/java.lang.reflect.Method.invoke(Method.java:566)
\tat org.apache.flink.api.python.shaded.py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
\tat org.apache.flink.api.python.shaded.py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
\tat org.apache.flink.api.python.shaded.py4j.Gateway.invoke(Gateway.java:282)
\tat org.apache.flink.api.python.shaded.py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
\tat org.apache.flink.api.python.shaded.py4j.commands.CallCommand.execute(CallCommand.java:79)
\tat org.apache.flink.api.python.shaded.py4j.GatewayConnection.run(GatewayConnection.java:238)
\tat java.base/java.lang.Thread.run(Thread.java:829)
",
      "stdout": "",
      "stderr": ""
    }
}
"""
