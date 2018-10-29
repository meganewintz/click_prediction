/* SimpleProg.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id


import scala.math.BigDecimal.RoundingMode
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.DataFrame
import Cleaner._
import shapeless.Tuple


object SimpleProg {
  def main(args: Array[String]) {
    val myFile = "data-students.json"
    val spark = SparkSession
      .builder()
      .appName("Click prediction")
      .config("spark.master", "local")
      .getOrCreate();

    import spark.implicits._

    val myData = spark.read.json(myFile)
    myData.createOrReplaceTempView("clicks")

    //myData.printSchema()
    //myData.show()

    //cleanAppOrSite(spark, myData)

    //cleanOS(spark, myData)

    //cleanLabel(spark, myData)

    //cleanExchange(spark, myData)

    //cleanMedia(spark, myData).show()

    //cleanPublisher(spark, myData).show()

    //cleanUser(spark, myData).show()

    val interest1 = cleanInterests(spark, myData)._1
    val interest2 = cleanInterests(spark, myData)._2

//    val s = cleanSize(spark, myData)
//    s.createOrReplaceTempView("sizes")
//    val size = spark.sql("SELECT size FROM sizes GROUP BY size ")
//    size.show()

    //cleanCity(spark, myData).show()
    //cleanType(spark, myData).show()

    //val clean = concat(cleanAppOrSite(spark, myData), cleanOS(spark, myData))
//    cleanAppOrSite(spark, myData)
//        .join(cleanOS(spark, myData))
//        .show()

    val cleanData = cleanAppOrSite(spark, myData).withColumn("id", monotonically_increasing_id())
        .join(cleanOS(spark, myData).withColumn("id", monotonically_increasing_id()), Seq("id"))
        .join(cleanLabel(spark, myData).withColumn("id", monotonically_increasing_id()), Seq("id"))
        .join(cleanExchange(spark, myData).withColumn("id", monotonically_increasing_id()), Seq("id"))
        .join(cleanMedia(spark, myData).withColumn("id", monotonically_increasing_id()), Seq("id"))
        .join(cleanPublisher(spark, myData).withColumn("id", monotonically_increasing_id()), Seq("id"))
        .join(cleanUser(spark, myData).withColumn("id", monotonically_increasing_id()), Seq("id"))
        .join(interest1.withColumn("id", monotonically_increasing_id()), Seq("id"))
        .join(interest2.withColumn("id", monotonically_increasing_id()), Seq("id"))
        //.join(cleanSize(spark, myData).withColumn("id", monotonically_increasing_id()), Seq("id"))
        .join(cleanCity(spark, myData).withColumn("id", monotonically_increasing_id()), Seq("id"))
        .join(cleanType(spark, myData).withColumn("id", monotonically_increasing_id()), Seq("id"))
        .drop("id")
//
    cleanData.show()




//    // Change bidfloor into float at 0.1
//    val bidfloor = myData.select($"bidfloor")
//
//    val newbidfloor = bidfloor.map( value => {
//      BigDecimal(value(0).toString).setScale(1, RoundingMode.HALF_UP)
//    })
//     //newbidfloor.show()
//
//    //Change bidfloor into int (the closest)
//
//      val bidfloor2 = myData.select($"bidfloor")
//
//    val newbidfloor2 = bidfloor2.map( value => {
//      BigDecimal(value(0).toString).setScale(0, RoundingMode.HALF_UP)
//    })
//    //newbidfloor2.show()


    spark.stop()
  }


}