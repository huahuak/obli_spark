package org.apache.spark.examples.sql.kaihua

import org.apache.spark.sql.SparkSession
import org.kaihua.obliop.Config
object TpcHTest {
  def main(args: Array[String]): Unit = {
    println("config: block number is " + Config.blockNumber)
    val spark = ObliviousSpark.getObliviousSpark(false)

    val part = spark.read
      .format("csv")
      .option("sep", "|")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(
        "file:///Users/huahua/Data/Projects/obli_spark/data/TPC-H\\ V3.0.1\\ 100mb/dbgen/part.csv"
      )

    val lineItem = spark.read
      .format("csv")
      .option("sep", "|")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(
        "file:///Users/huahua/Data/Projects/obli_spark/data/TPC-H\\ V3.0.1\\ 100mb/dbgen/lineitem.csv"
      )

    part.createTempView("part")
    lineItem.createTempView("line_item")
//    part.printSchema()
//    lineItem.printSchema()
//    part.show
//    lineItem.show

    spark
      .sql(
        "select /*+ MERGE(part, line_item) */ " +
          "part.P_PARTKEY, line_item.L_PARTKEY " +
//          "count(*) " +
          "from part join line_item on part.P_PARTKEY = line_item.L_PARTKEY"
      )
//      .show
      .write
      .format("csv")
      .option("sep", "|")
      .option("header", "true")
      .save(
        "file:///Users/huahua/Data/Projects/obli_spark/data/spark_100mb_result.csv"
      )
    Thread.sleep(60 * 60 * 1000)
  }
}
