package org.apache.spark.examples.sql.kaihua

import org.apache.spark.sql.SparkSession

object ObliviousSpark {
  def getObliviousSpark(obliviousEnabled: Boolean): SparkSession = {
    val sc = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("oblivious.enable", obliviousEnabled)
      .config("spark.sql.codegen.wholeStage", "false")
      .config(
        "spark.shuffle.sort.bypassMergeThreshold",
        "0" // disable bypass merge sort, default is 200
      )
//      .config("spark.sql.adaptive.enabled", false)
      .getOrCreate()

//    sc.sqlContext.experimental.extraStrategies =
//      Seq(ObliviousJoinStrategy) ++ sc.sqlContext.experimental.extraStrategies

    sc
  }
}
