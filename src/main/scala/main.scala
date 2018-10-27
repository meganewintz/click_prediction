/* SimpleProg.scala */
import org.apache.spark.sql.SparkSession

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
      .appName("Java Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate();

    import spark.implicits._

    val myData = spark.read.json(myFile)
    //myData.printSchema()
    //myData.show()

    myData.createOrReplaceTempView("clicks")

    //cleanAppOrSite(spark, myData).show()

    //cleanOS(spark, myData).show()

    //cleanLabel(spark, myData).show()

    //cleanExchange(spark, myData).show()

    //cleanMedia(spark, myData).show()

    //cleanPublisher(spark, myData).show()

    //cleanUser(spark, myData).show()

    //cleanInterests(spark, myData)._1.show()
    //cleanInterests(spark, myData)._2.show()

    //cleanSize(spark, myData).show()

    //cleanCity(spark, myData).show()
    cleanType(spark, myData).show()




    // Change bidfloor into float at 0.1
    val bidfloor = spark.sql("SELECT bidfloor FROM clicks")

    val newbidfloor = bidfloor.map( value => {
      BigDecimal(value(0).toString).setScale(1, RoundingMode.HALF_UP)
    })
     //newbidfloor.show()

    //Change bidfloor into int (the closest)

    val bidfloor2 = spark.sql("SELECT bidfloor FROM clicks")

    val newbidfloor2 = bidfloor2.map( value => {
      BigDecimal(value(0).toString).setScale(0, RoundingMode.HALF_UP)
    })
    //newbidfloor2.show()

    // Update type
    // null =>0, 0 => 1 , ..., CLICK => 5

    /*val stype = spark.sql("SELECT type FROM clicks")

    //println("------------------------------------ " + stype.first().toString)
    stype.show()
    val newtype = stype.map( value => {
    if (value(0) == "[null]") 0
    else {
    value(0).toString.head match {
        case '0' => 1
        case '1' => 2
        case '2' => 3
        case '3' => 4
        case _ => 5
      }
     }
    })
    newtype.show()*/


    //        val nbclass = appOrSite.filter(line => line.contains("app")).count()
    //        val nbjavascript = appOrSite.filter(line => line.contains("javascript")).count()
    //        println("Lignes avec javascript : %s, lignes avec class : %s".format(nbjavascript, nbclass))
    spark.stop()
  }


}