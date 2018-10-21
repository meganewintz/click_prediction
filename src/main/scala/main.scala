/* SimpleProg.scala */
import org.apache.spark.sql.SparkSession

import scala.math.BigDecimal.RoundingMode


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


    // Update appOrSite column
    // app   = 0
    // site  = 1
    // --------------------------------------
    val appOrSite = spark.sql("SELECT appOrSite FROM clicks")

    val newAppOrsite = appOrSite.map( value => {
      value(0) match {
        case "app" => 0
        case "site" => 1
      }
    } )
    // newAppOrsite.show()

    // Update os column
    // Android = 1
    // IOS = 2
    // Others = 0
    // --------------------------------------
    val os = spark.sql("SELECT os FROM clicks")

    val newOs = os.map( value => {
      value(0) match {
        case "Android" | "android" => 1
        case "iOS" | "ios" => 2
        case "Unknown" | null => 0
        case _ => 3
      }
    } )
    //newOs.show()

    // Update label column
    // false = 0
    // true = 1
    // --------------------------------------
    val label = spark.sql("SELECT label FROM clicks")

    val newLabel = label.map( value => {
      value(0) match {
        case true => 1
        case false => 0
      }
    } )
    //newLabel.show()

    // Update exchange column
    // 4 values so 0, 1, 2 or 3
    // --------------------------------------
    val exchange = spark.sql("SELECT exchange FROM clicks GROUP BY exchange")

    val newExchange = exchange.map( value => {
      if (value(0).toString.contains("f8dd")) 0
      else if (value(0).toString.contains("c7a327")) 1
      else if (value(0).toString.contains("46135")) 2
      else  3
    } )
    //newExchange.show()

    // Update media column
    // 2 values possible
    // if 343bc308e60156fb39cd2af57337a958 -> 0
    // else 1
    val media = spark.sql("SELECT media FROM clicks")

    val newMedia = media.map( value => {
      value(0) match {
        case "343bc308e60156fb39cd2af57337a958" => 0
        case _ => 1
      }
    } )
    // newMedia.show()

    // Update network column
    // 7 values possible according to the geographical situation of the country
    // 0 -> Unknown
    // 1 -> Europe
    // 2 -> Amérique du Nord et Centrale
    // 3 -> Asie
    // 4 -> Océanie
    // 5 -> Afrique
    // 6 -> Amérique du Sud/Latine
    val network = spark.sql("SELECT network FROM clicks")

    val newNetwork = network.map( value => {
      value(0).toString.head match {
        case '2' => 1
        case '3' => 2
        case '4' => 3
        case '5' => 4
        case '6' => 5
        case '7' => 6
        case _ => 0
      }
    })
    // newNetwork.show()

    /* Problem with this part*/
    // Update size column
    // 0 -> fullScreen / square
    // 1 -> horizontal
    // 2 -> vertical
   /* val size = spark.sql("SELECT size FROM clicks")

    val sizeToStr = size.map(_.toString)
    sizeToStr.show()
    val newSize = sizeToStr.map( value => {
      var array = value(0).toString.split(Array('(', ',', ' ', ')'))
      println("---------------------------------------------------------------------")
      println("COUCOU       " + array.size)
      println(array.mkString(""))
      /*val l = array.apply(1)
      val h = array.apply(3)
      (l, h) match {
        case ("300", "250") | ("200", "200") | ("250", "250") | ("336", "280") | ("480", "320") => 0
        case (l, h) if l.toInt > h.toInt => 1
        case _ => 3
      }*/

    })
    newSize.show()*/


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

    // Update publisher : not sure the starting char means something
    // starting by 0 -> 0 etc
    val publisher = spark.sql("SELECT publisher FROM clicks")

    val newpublisher = publisher.map( value => {
      value(0).toString.head match {
        case '0' => 0
        case '1' => 1
        case '2' => 2
        case '3' => 3
        case '4' => 4
        case '5' => 5
        case '6' => 6
        case '7' => 7
        case '8' => 8
        case '9' => 9
        case 'a' => 10
        case 'b' => 11
        case 'c' => 12
        case 'd' => 13
        case 'e' => 14
        case 'f' => 15
      }
    })
    newpublisher.show()

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

    //Update city
    // 0 if no city registered
    // else 1
    //val city = spark.sql("SELECT city FROM clicks")

   /* val newcity = city.map( value => {
    }*/



    //        val nbclass = appOrSite.filter(line => line.contains("app")).count()
    //        val nbjavascript = appOrSite.filter(line => line.contains("javascript")).count()
    //        println("Lignes avec javascript : %s, lignes avec class : %s".format(nbjavascript, nbclass))
    spark.stop()
  }


}