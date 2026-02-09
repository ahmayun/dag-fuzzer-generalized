# DAGger: DAG-Based Workload Generation for Testing Data-Intensive Computing Frameworks

DAGger is a testing framework for data-intensive computing (DISC) systems such as
Apache Spark and Flink. It generates executable dataflow programs by constructing
and progressively refining directed acyclic graphs (DAGs) of operators, enabling
systematic testing of query optimizers and execution engines beyond SQL-only
interfaces.

Unlike traditional SQL fuzzers, DAGger operates directly on logical dataflow
graphs, aligning test generation with the internal representations used by modern
DISC frameworks.

---

## High-Level Overview

DAGger generates test programs in three stages:

1. **DAG Generation**  
   Generate a context-free directed acyclic graph that captures only the shape of
   a dataflow pipeline, without committing to specific operators or parameters.

2. **Abstract Dataflow Graph (ADFG) Construction**  
   Assign each node in the DAG a concrete operator (e.g., `read`, `filter`, `join`)
   using a machine-readable API specification, producing an abstract dataflow
   graph.

3. **State-Aware Parameterization**  
   Fill in operator parameters (e.g., column names, predicates, join keys) using
   a propagated schema state, yielding a concrete, executable program.

This staged design separates structural generation, operator selection, and
semantic validation, making DAGger modular and extensible across frameworks.

---

## Architecture

At a high level, DAGger consists of the following components:

```
      +--------------------+
      |  Stage 1           |
      |  DAG Generation    |
      |  (Structure Only)  |
      +----------+---------+
                 |
                 v
      +--------------------+
      |  Stage 2           |
      |  Operator          |
      |  Assignment (ADFG) |
      +----------+---------+
                 |
                 v
      +--------------------+
      |  Stage 3           |
      |  State-Aware       |
      |  Parameterization  |
      +----------+---------+
                 |
                 v
      +--------------------+
      |  Code Generation   |
      |  & Execution       |
      +--------------------+
```

## Currently Supported Frameworks

DAGger currently supports the following dataflow frameworks and language bindings:

- **Apache Spark (Scala)**
- **Apache Flink (Python)**
- **Dask (Python)**
- **Polars (Python)**

Support is backend-specific and implemented via separate execution and code
generation modules.

---

## Extending DAGger to New Frameworks

Adding support for a new dataflow framework involves two main steps:

1. **Environment Integration**  
   Framework-specific execution logic (e.g., job submission, runtime setup,
   result handling) should be added under
   `src/main/scala/fuzzer/framework`.

2. **Code Generation**  
   Lowering an abstract dataflow graph (ADFG) to executable code for a target
   framework is handled by adapter modules located in
   `src/main/scala/fuzzer/adapters`.
   Each subdirectory corresponds to a supported framework and language binding.

This separation allows DAGger to remain modular: the core fuzzing logic is
framework-agnostic, while backend-specific behavior is isolated in adapters and
framework integration layers.

## Real-World Impact

DAGger has uncovered multiple previously unknown bugs across widely used
dataflow frameworks. Several of these issues have been confirmed by framework
developers, demonstrating that DAGger’s generated workloads exercise optimizer
and execution paths that are difficult to reach with existing testing tools.

### Reported Issues

| Framework | Issue ID | Status |
|---------|---------|--------|
| Flink | [FLINK-38397](https://issues.apache.org/jira/browse/FLINK-38397) | Confirmed |
| Polars | [#25971](https://github.com/pola-rs/polars/issues/25971) | Confirmed |
| Dask | [#12257](https://github.com/dask/dask/issues/12257) | Confirmed |
| Polars | [#26322](https://github.com/pola-rs/polars/issues/26322) | Confirmed |
| Spark | [SPARK-51798](https://issues.apache.org/jira/browse/SPARK-51798) | No response |
| Spark | [SPARK-54196](https://issues.apache.org/jira/browse/SPARK-54196) | No response |
| Flink | [FLINK-38366](https://issues.apache.org/jira/browse/FLINK-38366) | No response |
| Flink | [FLINK-38446](https://issues.apache.org/jira/browse/FLINK-38446) | No response |
| Flink | [FLINK-38637](https://issues.apache.org/jira/browse/FLINK-38637) | No response |

