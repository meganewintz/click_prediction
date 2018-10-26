/* SimpleProg.scala */
import org.apache.spark.sql.SparkSession
import scala.math.BigDecimal.RoundingMode
import org.apache.spark.ml.feature.StringIndexer

import Cleaner._


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

    //cleanInterests(spark, myData).show()

    //cleanSize(spark, myData).show()


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
        case _   => 0
      }
    })
    //newNetwork.show()
   // val network = myData.select($"network")
//      network.show()

    /* Problem with this part*/
    // Update size column
    // 0 -> fullScreen / square
    // 1 -> horizontal
    // 2 -> vertical
    val size = spark.sql("SELECT size FROM clicks")

    val sizeToStr = size.map(_.toString)

    val newSize = sizeToStr.map( value => {
      val a = value.toString.split(Array('(', ',' , ' ', ')'))
      val l = a.apply(1)
      val h = a.apply(3)
      (l, h) match {
        case ("300", "250") | ("200", "200") | ("250", "250") | ("336", "280") | ("480", "320") => 0
        case (l, h) if l.toInt > h.toInt => 1
        case _ => 2
      }
    })
    //newSize.show()



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

    //Update city
    // 0 if no city registered
    // else 1
    //val city = spark.sql("SELECT city FROM clicks")

   /* val newcity = city.map( value => {
    }*/
    

    //interests => 1 col for each interest


    /*val interestsToStr = interests.map(_.toString)

    val counts = interestsToStr.rdd.flatMap(line => line.toString.split(","))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)*/

    val interests = spark.sql("SELECT interests FROM clicks")

    val interestsToStr = interests.map(_.toString)

    val listInterests = interestsToStr.map( value => {
      val listInit = List.fill(26)(0)
      var array = listInit.toArray
      val newI = value.toString.toLowerCase
      val iab1 = List("iab1", "entertainment", "book", "literature", "celebrity", "gossip", "fine art", "humor", "movie", "music", "television")
      val iab2 = List("iab2", "automotive", "auto parts", "auto repair", "buying/selling cars", "car culture", "certified pre-owned", "convertible", "coupe", "crossover", "diesel", "vehicle", "pickup", "road-side", "sedan", "truck", "vintage car", "wagon")
      val iab3 = List("iab3", "business", "advertising", "agriculture", "biotech", "biomedical", "business software", "construction", "forestry", "government", "green solution", "human ressource", "logistic", "marketing", "matal")
      val iab4 = List("iab3", "career", "college", "financial aid", "job", "resume writing/advice", "nursing", "scholarships", "telecommuting", "military")
      val iab5 = List("iab5", "education")
      val iab6 = List("iab6", "family", "parenting")
      val iab7 = List("iab7", "health", "fitness")
      val iab8 = List("iab8", "food", "drink")
      val iab9 = List("iab9", "hobbies", "interest")
      val iab10 = List("iab10", "home", "garden")
      val iab11 = List("iab11", "law", "politics")
      val iab12 = List("iab12", "news")
      val iab13 = List("iab13", "personal finance")
      val iab14 = List("iab14", "society")
      val iab15 = List("iab15", "science")
      val iab16 = List("iab16", "pet")
      val iab17 = List("iab17", "sport")
      val iab18 = List("iab18", "style", "fashion")
      val iab19 = List("iab19", "technology", "computing")
      val iab20 = List("iab20", "travel")
      val iab21 = List("iab21", "real estate")
      val iab22 = List("iab22", "shopping")
      val iab23 = List("iab23", "religion", "spirituality")
      val iab24 = List("iab24", "uncategorized")
      val iab25 = List("iab25", "non-standard content")
      val iab26 = List("iab26", "illegal content")

      if (iab1.exists(newI.contains)){
        array(0) = 1
      }
      if (iab2.exists(newI.contains)){
        array(1) = 1
      }
      if (iab3.exists(newI.contains)){
        array(2) = 1
      }
      if (iab4.exists(newI.contains)){
        array(3) = 1
      }
      if (iab5.exists(newI.contains)){
        array(4) = 1
      }
      if (iab6.exists(newI.contains)){
        array(5) = 1
      }
      if (iab7.exists(newI.contains)){
        array(6) = 1
      }
      if (iab8.exists(newI.contains)){
        array(7) = 1
      }
      if (iab9.exists(newI.contains)){
        array(8) = 1
      }
      if (iab10.exists(newI.contains)){
        array(9) = 1
      }
      if (iab11.exists(newI.contains)){
        array(10) = 1
      }
      if (iab12.exists(newI.contains)){
        array(11) = 1
      }
      if (iab13.exists(newI.contains)){
        array(12) = 1
      }
      if (iab14.exists(newI.contains)){
        array(13) = 1
      }
      if (iab15.exists(newI.contains)){
        array(14) = 1
      }
      if (iab16.exists(newI.contains)){
        array(15) = 1
      }
      if (iab17.exists(newI.contains)){
        array(16) = 1
      }
      if (iab18.exists(newI.contains)){
        array(17) = 1
      }
      if (iab19.exists(newI.contains)){
        array(18) = 1
      }
      if (iab20.exists(newI.contains)){
        array(19) = 1
      }
      if (iab21.exists(newI.contains)){
        array(20) = 1
      }
      if (iab22.exists(newI.contains)){
        array(21) = 1
      }
      if (iab23.exists(newI.contains)){
        array(22) = 1
      }
      if (iab24.exists(newI.contains)){
        array(23) = 1
      }
      if (iab25.exists(newI.contains)){
        array(24) = 1
      }
      if (iab26.exists(newI.contains)){
        array(25) = 1
      }

      array

      /*val a = value.toString.split(Array('(', ',' , ' ', ')'))
      val l = a.apply(1)
      val h = a.apply(3)
      (l, h) match {
        case ("300", "250") | ("200", "200") | ("250", "250") | ("336", "280") | ("480", "320") => 0
        case (l, h) if l.toInt > h.toInt => 1
        case _ => 2*/
      })
    listInterests.show()






    /*val newInterests = interestsToStr.map( value => {
      val a = value.toString.split(Array('(', ',' , ' ', ')'))
      val l = a.apply(1)
      val h = a.apply(3)
      (l, h) match {
        case ("300", "250") | ("200", "200") | ("250", "250") | ("336", "280") | ("480", "320") => 0
        case (l, h) if l.toInt > h.toInt => 1
        case _ => 2
      }*/

    //        val nbclass = appOrSite.filter(line => line.contains("app")).count()
    //        val nbjavascript = appOrSite.filter(line => line.contains("javascript")).count()
    //        println("Lignes avec javascript : %s, lignes avec class : %s".format(nbjavascript, nbclass))
    spark.stop()
  }


}