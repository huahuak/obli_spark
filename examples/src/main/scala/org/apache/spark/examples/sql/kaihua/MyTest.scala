package org.apache.spark.examples.sql.kaihua

import org.apache.spark.sql.SparkSession

/**
 * @author kahua.li 
 * @email moflowerlkh@gmail.com
 * @date 2022/12/30
 **/

object MyTest {
  case class People(name: String, age: Int)

  case class Car(id: Long, owner: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .master("local")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.shuffle.sort.bypassMergeThreshold", "0") // disable bypass merge sort, default is 200
        .getOrCreate()
    import spark.implicits._
    val peopleDS = Seq(
      People("LiHua", 22),
      People("XYue", 18)
    ).toDS()
    val carDS = Seq(
      Car(101, "LiHua"),
      Car(102, "XYue"),
    ).toDS()
    peopleDS.createTempView("people")
    carDS.createTempView("car")

    if (1 == 1) {
      spark.sql("select  /*+ SHUFFLE_HASH(people) */ * " +
          "from people join car on people.name = car.owner")
          .show()
    } else {
      spark.sql("select avg(age) from people").show() 
    }
    Thread.sleep(60 * 60 * 1000)
  }
}
