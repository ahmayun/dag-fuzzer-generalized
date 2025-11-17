import org.apache.spark.sql.{SparkSession, DataFrame}
import java.io.File

// Configuration
val inputBasePath = "tpcds-data"
val samplePercentage = 0.05  // 10% of data (adjust between 0.0 and 1.0)
val suffix = s"${(samplePercentage*100).toInt}pc"
val outputParquetBasePath = s"tpcds-data-${suffix}"
val outputCSVBasePath = s"tpcds-csv-${suffix}"

// Function to get all subdirectories (table names)
def getTableDirectories(basePath: String): Array[String] = {
  val baseDir = new File(basePath)
  if (baseDir.exists && baseDir.isDirectory) {
    baseDir.listFiles
      .filter(_.isDirectory)
      .map(_.getName)
  } else {
    Array.empty[String]
  }
}

// Function to sample and export a single table
def sampleAndExportTable(tableName: String): Unit = {
  val inputPath = s"$inputBasePath/$tableName"
  val outputParquetPath = s"$outputParquetBasePath/$tableName"
  val outputCSVPath = s"$outputCSVBasePath/$tableName"

  try {
    println(s"Processing table: $tableName")

    // Read parquet files
    val df = spark.read.parquet(inputPath)
    val originalCount = df.count()

    // Random sample
    val sampledDF = df.sample(withReplacement = false, fraction = samplePercentage, seed = 42)
    val sampledCount = sampledDF.count()

    println(s"  - Original row count: $originalCount")
    println(s"  - Sampled row count: $sampledCount (${(samplePercentage * 100).toInt}%)")
    println(s"  - Schema: ${df.columns.mkString(", ")}")

    // Write as Parquet
    sampledDF
      .write
      .mode("overwrite")
      .parquet(outputParquetPath)

    println(s"  - Saved Parquet to: $outputParquetPath")

    // Write as CSV with header
    sampledDF.coalesce(1)  // Single file output
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputCSVPath)

    println(s"  - Saved CSV to: $outputCSVPath")

  } catch {
    case e: Exception =>
      println(s"  - Error processing $tableName: ${e.getMessage}")
  }
}

// Main sampling process
println("Starting TPC-DS data sampling and export...")
println(s"Input path: $inputBasePath")
println(s"Output Parquet path: $outputParquetBasePath")
println(s"Output CSV path: $outputCSVBasePath")
println(s"Sample percentage: ${(samplePercentage * 100).toInt}%")
println()

// Get all table directories
val tables = getTableDirectories(inputBasePath)

if (tables.isEmpty) {
  println(s"No table directories found in $inputBasePath")
} else {
  println(s"Found ${tables.length} tables: ${tables.mkString(", ")}")
  println()

  // Process each table
  tables.foreach { table =>
    sampleAndExportTable(table)
    println()
  }

  println("Sampling and export completed!")
}

// Optional: Show summary statistics
println("\nSummary:")
println(s"Parquet output directory: $outputParquetBasePath")
println(s"CSV output directory: $outputCSVBasePath")

val parquetDir = new File(outputParquetBasePath)
val csvDir = new File(outputCSVBasePath)

if (parquetDir.exists()) {
  val parquetTables = parquetDir.listFiles().filter(_.isDirectory).length
  println(s"  - Tables in Parquet format: $parquetTables")
}

if (csvDir.exists()) {
  val csvTables = csvDir.listFiles().filter(_.isDirectory).length
  println(s"  - Tables in CSV format: $csvTables")
}