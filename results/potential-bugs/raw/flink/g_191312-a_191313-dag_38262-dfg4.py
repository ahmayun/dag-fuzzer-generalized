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

autonode_6 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_5 = autonode_6.select(col('i_manufact_id_node_6'))
autonode_4 = autonode_5.distinct()
autonode_3 = autonode_4.group_by(col('i_manufact_id_node_6')).select(col('i_manufact_id_node_6').avg.alias('i_manufact_id_node_6'))
autonode_2 = autonode_3.distinct()
autonode_1 = autonode_2.group_by(col('i_manufact_id_node_6')).select(col('i_manufact_id_node_6').sum.alias('i_manufact_id_node_6'))
sink = autonode_1.group_by(col('i_manufact_id_node_6')).select(col('i_manufact_id_node_6').count.alias('i_manufact_id_node_6'))
print(sink.explain())

# ======== Details ========
"""
{
  "is_same": true,
  "result_name": "IndexOutOfBoundsException",
  "result_details": {
    "opt": {
      "success": false,
      "error_name": "IndexOutOfBoundsException",
      "error_message": "An error occurred while calling o104274233.explain.
: java.lang.IndexOutOfBoundsException: Index 1 out of bounds for length 1
\tat java.base/jdk.internal.util.Preconditions.outOfBounds(Preconditions.java:64)
\tat java.base/jdk.internal.util.Preconditions.outOfBoundsCheckIndex(Preconditions.java:70)
\tat java.base/jdk.internal.util.Preconditions.checkIndex(Preconditions.java:248)
\tat java.base/java.util.Objects.checkIndex(Objects.java:374)
\tat java.base/java.util.ArrayList.get(ArrayList.java:459)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange.lambda$createHashPartitioner$2(BatchExecExchange.java:241)
\tat java.base/java.util.stream.IntPipeline$1$1.accept(IntPipeline.java:180)
\tat java.base/java.util.Spliterators$IntArraySpliterator.forEachRemaining(Spliterators.java:1032)
\tat java.base/java.util.Spliterator$OfInt.forEachRemaining(Spliterator.java:699)
\tat java.base/java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:484)
\tat java.base/java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:474)
\tat java.base/java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:550)
\tat java.base/java.util.stream.AbstractPipeline.evaluateToArrayNode(AbstractPipeline.java:260)
\tat java.base/java.util.stream.ReferencePipeline.toArray(ReferencePipeline.java:517)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange.createHashPartitioner(BatchExecExchange.java:242)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange.translateToPlanInternal(BatchExecExchange.java:209)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.translateToPlan(ExecEdge.java:262)
\tat org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc.translateToPlanInternal(CommonExecCalc.java:94)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.translateToPlan(ExecEdge.java:262)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange.translateToPlanInternal(BatchExecExchange.java:161)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.translateToPlan(ExecEdge.java:262)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate.translateToPlanInternal(BatchExecHashAggregate.java:167)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.translateToPlan(ExecEdge.java:262)
\tat org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc.translateToPlanInternal(CommonExecCalc.java:94)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.delegation.BatchPlanner.$anonfun$translateToPlan$1(BatchPlanner.scala:95)
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
\tat org.apache.flink.table.planner.delegation.BatchPlanner.translateToPlan(BatchPlanner.scala:94)
\tat org.apache.flink.table.planner.delegation.PlannerBase.getExplainGraphs(PlannerBase.scala:627)
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
",
      "stdout": "",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "IndexOutOfBoundsException",
      "error_message": "An error occurred while calling o104274351.explain.
: java.lang.IndexOutOfBoundsException: Index 1 out of bounds for length 1
\tat java.base/jdk.internal.util.Preconditions.outOfBounds(Preconditions.java:64)
\tat java.base/jdk.internal.util.Preconditions.outOfBoundsCheckIndex(Preconditions.java:70)
\tat java.base/jdk.internal.util.Preconditions.checkIndex(Preconditions.java:248)
\tat java.base/java.util.Objects.checkIndex(Objects.java:374)
\tat java.base/java.util.ArrayList.get(ArrayList.java:459)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange.lambda$createHashPartitioner$2(BatchExecExchange.java:241)
\tat java.base/java.util.stream.IntPipeline$1$1.accept(IntPipeline.java:180)
\tat java.base/java.util.Spliterators$IntArraySpliterator.forEachRemaining(Spliterators.java:1032)
\tat java.base/java.util.Spliterator$OfInt.forEachRemaining(Spliterator.java:699)
\tat java.base/java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:484)
\tat java.base/java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:474)
\tat java.base/java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:550)
\tat java.base/java.util.stream.AbstractPipeline.evaluateToArrayNode(AbstractPipeline.java:260)
\tat java.base/java.util.stream.ReferencePipeline.toArray(ReferencePipeline.java:517)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange.createHashPartitioner(BatchExecExchange.java:242)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange.translateToPlanInternal(BatchExecExchange.java:209)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.translateToPlan(ExecEdge.java:262)
\tat org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc.translateToPlanInternal(CommonExecCalc.java:94)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.translateToPlan(ExecEdge.java:262)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange.translateToPlanInternal(BatchExecExchange.java:161)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.translateToPlan(ExecEdge.java:262)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate.translateToPlanInternal(BatchExecHashAggregate.java:167)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.translateToPlan(ExecEdge.java:262)
\tat org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc.translateToPlanInternal(CommonExecCalc.java:94)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.delegation.BatchPlanner.$anonfun$translateToPlan$1(BatchPlanner.scala:95)
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
\tat org.apache.flink.table.planner.delegation.BatchPlanner.translateToPlan(BatchPlanner.scala:94)
\tat org.apache.flink.table.planner.delegation.PlannerBase.getExplainGraphs(PlannerBase.scala:627)
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
",
      "stdout": "",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0