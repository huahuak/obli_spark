package org.apache.spark.examples.sql.kaihua

import org.apache.spark.sql.SparkSession

/** @author
  *   kahua.li
  * @email
  *   moflowerlkh@gmail.com
  * @date
  *   2023/01/13
  */
object MyTest {
  case class People(name: String, age: Int)

  case class Car(owner: String, id: Int)

  def main(args: Array[String]): Unit = {
    val spark = ObliviousSpark.getObliviousSpark(false)

    import spark.implicits._
    val peopleDS = Seq(
      People("LiHua", 22),
      People("XYue", 18)
    ).toDS()
    val carDS = Seq(
      Car("LiHua", 101),
      Car("XYue", 102)
    ).toDS()
    peopleDS.createTempView("people")
    carDS.createTempView("car")

    if (1 == 1) {
      spark
        .sql(
          "select  /*+ MERGE(people) */ * " +
            "from people join car on people.name = car.owner"
        )
        .show()
    } else {
      spark.sql("select avg(age) from people").show()
    }
    Thread.sleep(60 * 60 * 1000)
  }
}
