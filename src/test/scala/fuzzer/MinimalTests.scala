package fuzzer

import fuzzer.MainFuzzer.createFuzzerEngine
import fuzzer.UserImplSparkScala.dag2Scala
import fuzzer.adapters.spark.{SparkCodeExecutor, SparkCodeGenerator, SparkDataAdapter}
import fuzzer.core.engine.FuzzerEngine
import fuzzer.core.global.FuzzerConfig
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.IntegerType
import fuzzer.factory.AdapterFactory
import fuzzer.utils.io.ReadWriteUtils.{createDir, deleteDir, prettyPrintStats, saveResultsToFile}
import fuzzer.utils.json.JsonReader
import fuzzer.utils.random.Random
import org.junit.Test
import org.junit.Assert._

class MinimalTests {

  @Test
  def testSparkScalaGen(): Unit = {
    val config = FuzzerConfig.getSparkScalaConfig
      .copy(seed = 123)

    val spec = JsonReader.readJsonFile(config.specPath)

    fuzzer.core.global.State.config = Some(config)
    Random.setSeed(config.seed)

    // Clean output directories
    deleteDir(config.dagGenDir)
    deleteDir(config.outDir)
    createDir(config.outDir)

    // Create API-specific components using factory
    val dataAdapter = new SparkDataAdapter(config)
    val codeGenerator = new SparkCodeGenerator(config, spec)
    val codeExecutor = new SparkCodeExecutor(config, spec)

    val engine = new FuzzerEngine(
      config = config,
      spec = spec,
      dataAdapter = dataAdapter,
      codeGenerator = codeGenerator,
      codeExecutor = codeExecutor
    )

    val dag = engine.generateSingleDAG()
    println(dag)

    val hardcodedTables: List[TableMetadata] = List(
      TableMetadata(
        _identifier = "users",
        _columns = Seq(
          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
        ),
        _metadata = Map("source" -> "auth_system")
      ),
      TableMetadata(
        _identifier = "orders",
        _columns = Seq(
          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
        ),
        _metadata = Map("source" -> "ecommerce")
      ),
      TableMetadata(
        _identifier = "products",
        _columns = Seq(
          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
        ),
        _metadata = Map("source" -> "inventory")
      )
    )
    val dag2ScalaFunc = UserImplSparkScala.dag2Scala(spec) _
    val sourceCode = engine.generateSingleProgram(dag, spec, dag2ScalaFunc, hardcodedTables)
    println(sourceCode)
    assert(true)
  }

  @Test
  def testBeamJavaGen(): Unit = {
    assert(true)
  }

  @Test
  def testTensorflowPythonGen(): Unit = {
    assert(true)
  }
}
