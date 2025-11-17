package fuzzer.utils.spark.tpcds

import fuzzer.utils.io.ReadWriteUtils._
import org.apache.spark.sql.SparkSession

object TPCDSTablesLoader {

  def loadAll(
               spark: SparkSession, tpcdsDataPath: String,
               dbName: String = "main",  filterF: String => Boolean = _ => true): Unit = {


    deleteDir(s"spark-warehouse/$dbName.db")

    // 1. Make sure "main" database exists
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    // 2. List of all your table names
    val tableNames = Seq(
      "call_center",
      "catalog_page",
      "catalog_returns",
      "catalog_sales",
      "customer",
      "customer_address",
      "customer_demographics",
      "date_dim",
      "household_demographics",
      "income_band",
      "inventory",
      "item",
      "promotion",
      "reason",
      "ship_mode",
      "store",
      "store_returns",
      "store_sales",
      "time_dim",
      "warehouse",
      "web_page",
      "web_returns",
      "web_sales",
      "web_site"
    ).filter(filterF)

    // 3. Read each Parquet file, create temp view
    tableNames.foreach { tableName =>
      println(s"reading table $tableName...")
      spark.read.parquet(s"$tpcdsDataPath/$tableName").createOrReplaceTempView(tableName)
    }

    // 4. Promote each temp view to a managed table inside "main"
    tableNames.foreach { tableName =>
      println(s"Creating table in sparksql $tableName...")
      spark.sql(s"CREATE TABLE $dbName.$tableName USING parquet AS SELECT * FROM $tableName")
    }

  }

}

