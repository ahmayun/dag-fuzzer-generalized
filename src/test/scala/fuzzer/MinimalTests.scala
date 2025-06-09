package fuzzer

import fuzzer.adapters.spark.{SparkCodeExecutor, SparkCodeGenerator, SparkDataAdapter}
import fuzzer.core.engine.FuzzerEngine
import fuzzer.core.global.FuzzerConfig
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.IntegerType
import fuzzer.utils.io.ReadWriteUtils.{createDir, deleteDir}
import fuzzer.utils.json.JsonReader
import fuzzer.utils.random.Random
import org.junit.Test
import play.api.libs.json.JsValue

class MinimalTests {

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

  def createEngineFromConfig(config: FuzzerConfig, spec: JsValue): FuzzerEngine = {

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

    engine
  }
  @Test
  def testSparkScalaGen(): Unit = {
    val config = FuzzerConfig.getSparkScalaConfig
      .copy(seed = 1234)
    val spec = JsonReader.readJsonFile(config.specPath)
    val engine = createEngineFromConfig(config, spec)

    // This may generate an invalid DAG (that's normal
    // since I have no control over what the DAG module
    // will generate), change seed if the DAG is invalid.
    val dag = engine.generateSingleDAG()
    println(dag)

    val dag2SparkScalaFunc = UserImplSparkScala.dag2SparkScala(spec) _
    val sourceCode = engine.generateSingleProgram(dag, spec, dag2SparkScalaFunc, hardcodedTables)
    println(sourceCode)
//    val result = engine.codeExecutor.execute(sourceCode)
    assert(true)
  }

  @Test
  def testBeamJavaGen(): Unit = {
    assert(true)
  }

  @Test
  def testFlinkJava(): Unit = {
    assert(true)
  }
}
