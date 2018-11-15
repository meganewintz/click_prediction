import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.ml.feature.VectorAssembler

object Cleaner {


  /**
    * Return a new dataframe containing all the values cleaned
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return dataframe of cleaned values
    */
  def cleanData(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    // Reduce the number of data to have a better proportion between label = true or false.
    val trueValue = data.filter("label = true")
    val falseValue = data.filter("label = false").sample(0.035)

    val sampleData = trueValue.union(falseValue)


    // Call all the function to clean the data
    val appOrSite = cleanAppOrSite(spark, sampleData)
    val network   = cleanNetwork(spark, sampleData)
    val location  = cleanLocation(spark, sampleData)
    val os        = cleanOS(spark, sampleData)
    val exchange  = cleanExchange(spark, sampleData)
    val media     = cleanMedia(spark, sampleData)
    val bidfloor  = cleanBidFloor(spark, sampleData)
    val publisher = cleanPublisher(spark, sampleData)
    val user      = cleanUser(spark, sampleData)
    val interest1 = cleanInterests(spark, sampleData)._1
    val interest2 = cleanInterests(spark, sampleData)._2
    val size      = cleanSize(spark, sampleData)
    val city      = cleanCity(spark, sampleData)
    val types     = cleanType(spark, sampleData)
    val label     = cleanLabel(spark, sampleData)


    // Put all the clean data on a same dataframe (and drop no-significant variables)
    val cleanData =
      appOrSite.withColumn("id", monotonically_increasing_id())
          //.join(network.withColumn("id", monotonically_increasing_id()), Seq("id"))
          //.join(location.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .join(os.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .join(exchange.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .join(media.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .join(bidfloor.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .join(publisher.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .join(user.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .join(interest1.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .join(interest2.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .join(size.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .join(city.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .join(types.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .join(label.withColumn("id", monotonically_increasing_id()), Seq("id"))
          .drop("id")
          .drop("i4")
          .drop("i7")
          .drop("i9")
          .drop("i11")
          .drop("i14")
          .drop("i16")
          .drop("i17")
          .drop("i18")
          .drop("i20")
          .drop("i23")
          .drop("i24")
          .drop("i26")

    // Create the features col as a Vector.
    val featuresCols = Array("appOrSite", "os", "exchange", "media", "bidfloor", "publisher", "user", "i1", "i2","i3", "i5", "i6","i8","i10","i12", "i13", "i15", "i19", "i21", "i22", "i25", "size", "city", "type")
     val assembler = new VectorAssembler()
          .setInputCols(featuresCols)
          .setOutputCol("features")

    // Keep only the two intersting columns : features and label.
    val output = assembler.transform(cleanData).select( $"features", $"label")
    output
  }

  /**
    * Return a DataFrame containing value of the geographical area from which the user uses the app or site
    *
    * Different geographical area:
    * Europe + Israel = 1
    * North and Central America = 2
    * Asia = 3
    * Oceania + Malaisia = 4
    * Africa = 5
    * South America = 6
    * Unknown = 0
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return Dataframe of cleaned values
    */
  def cleanNetwork(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val network = data.select($"network")

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
    newNetwork.toDF("networkZone")
  }

  /** NEED TO BE CHECK : croiser avec appOrSite pour savoir s'il est chez lui ou dehors?
    *
    * Return a DataFrame containing value of the place from where the user uses the app
    *
    * Different locations :
    * home = 2
    * outside = 1
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return Dataframe of cleaned values
    */
  def cleanLocation(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val network = data.select($"network")

    val newLocation = network.map( value => {
      value(0) match {
        case null => 2
        case _ => 1
      }
    })
    newLocation.toDF("networkLocation")
  }

  /**
    * Return a DataFrame containing one column "label" cleaned.
    *
    * Different label values :
    * false = 0
    * true = 1
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return a DataFrame
    */
  def cleanLabel(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val label = data.select($"label")


    val newLabel = label.map( value => {
      value(0) match {
        case true => 1.0
        case false => 0.0
      }
    } )
    newLabel.toDF("label")
  }

  /**
    * Return a DataFrame containing one column "os" cleaned.
    *
    * Different os values :
    * Unknown = 0
    * Android = 1
    * IOS = 2
    * Others = 3
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return a DataFrame
    */
  def cleanOS(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val os = data.select($"os")

    val newOs = os.map( value => {
      value(0) match {
        case "Unknown" | null => 0
        case "Android" | "android" => 1
        case "iOS" | "ios" => 2
        case _ => 3
      }
    })
    newOs.toDF("os")
  }

  /**
    * Return a DataFrame containing one column "appOrSite" cleaned.
    *
    * Different appOrSite values :
    * app   = 1
    * site  = 2
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return a DataFrame
    */
  def cleanAppOrSite(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val appOrSite = data.select($"appOrSite")

    val newAppOrsite = appOrSite.map( value => {
      value(0) match {
        case "app" => 1
        case "site" => 2
      }
    } )

    newAppOrsite.toDF("appOrSite")
  }

  /**
    * Return a DataFrame which contain one column "exchange" cleaned.
    *
    * 4 different exchange values :
    * 1, 2 or 3, 4.
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return a DataFrame
    */
  def cleanExchange(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val exchange = data.select($"exchange")

    val newExchange = exchange.map( value => {
      if (value(0).toString.contains("f8dd")) 1
      else if (value(0).toString.contains("c7a327")) 2
      else if (value(0).toString.contains("46135")) 3
      else  4
    } )
    newExchange.toDF("exchange")
  }

  /**
    * Return a DataFrame containing one column "media" cleaned.
    *
    * 2 different exchange values :
    * if 343bc308e60156fb39cd2af57337a958 -> 1
    * else 2
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return a DataFrame
    */
  def cleanMedia(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val media = data.select($"media")

    val newMedia = media.map( value => {
          value(0) match {
            case "343bc308e60156fb39cd2af57337a958" => 1
            case _ => 2
          }
        })
        newMedia.toDF("media")
  }


  /**
    * Return a DataFrame containing one column "bidfloor" cleaned.
    *
    * if null -> 0.0
    * else, initial value (Double)
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return a DataFrame
    */
  def cleanBidFloor(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val bidfloor = data.select($"bidfloor")

    val newBidfloor = bidfloor.map( value => {
      value(0) match {
        case null => 0.0
        case _ => value(0).toString.toDouble
      }
    })

    newBidfloor.toDF("bidfloor")

  }

  /**
    * Return a DataFrame containing one column "publisher" cleaned.
    *
    * Different publisher values : not sure the starting char means something
    * starting by 0 -> 1 etc
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return a DataFrame
    */
  def cleanPublisher(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._


    val publisher = data.select($"publisher")

    val newpublisher = publisher.map( value => {
      value(0).toString.head match {
        case '0' => 1
        case '1' => 2
        case '2' => 3
        case '3' => 4
        case '4' => 5
        case '5' => 6
        case '6' => 7
        case '7' => 8
        case '8' => 9
        case '9' => 10
        case 'a' => 11
        case 'b' => 12
        case 'c' => 13
        case 'd' => 14
        case 'e' => 15
        case 'f' => 16
      }
    })
    newpublisher.toDF("publisher")
  }

  /**
    * Return a DataFrame containing one column "user" cleaned.
    *
    * Different user values :
    * an int corresponding to a user
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return a dataFrame
    */
  def cleanUser(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val user = data.select($"user")
    val si = new StringIndexer().setInputCol("user").setOutputCol("user-num")
    val sm = si.fit(user)
    val newUser = sm.transform(user).drop("user")
    val nUser = newUser.map(user => user(0).toString.split('.')(0).toInt)

    nUser.toDF("user")
  }

  /**
    * Return a DataFrame containing a column per interest.
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return a DataFrame
    */
  def cleanInterests(spark: SparkSession, data: DataFrame): (DataFrame, DataFrame) = {
    import spark.implicits._
    /**
      * Return 1 or 2 according to the presence of an interest
      *
      * @param iab the list of string corresponding to the interest
      * @param interests interests of a user
      * @return 2 if one of the string of iab is in interests else 1
      */
    def hasInterest(iab: List[String], interests: String): Int = {
      val interestsComa = interests + ","
      val interestsDash = interests + "-"
      if (iab.exists(interestsComa.contains)||iab.exists(interestsDash.contains)){
        2
      }
      else 1
    }

    val interests = data.select($"interests")
    val interestsToStr = interests.map(_.toString)

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

    val listInterests1 = interestsToStr.map( value => {
      // val listInit = List.fill(26)(0)
      var tuple13 = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
      val newI = value.toString.toLowerCase

      tuple13 = tuple13.copy(
        _1 = hasInterest(iab1, newI),
        _2 = hasInterest(iab2, newI),
        _3 = hasInterest(iab3, newI),
        _4 = hasInterest(iab4, newI),
        _5 = hasInterest(iab5, newI),
        _6 = hasInterest(iab6, newI),
        _7 = hasInterest(iab7, newI),
        _8 = hasInterest(iab8, newI),
        _9 = hasInterest(iab9, newI),
        _10 = hasInterest(iab10, newI),
        _11 = hasInterest(iab11, newI),
        _12 = hasInterest(iab12, newI),
        _13 = hasInterest(iab13, newI))
      tuple13
    })

    val listInterests2 = interestsToStr.map( value => {
      // val listInit = List.fill(26)(0)
      var tuple13 = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
      val newI = value.toString.toLowerCase

      tuple13 = tuple13.copy(
        _1 = hasInterest(iab14, newI),
        _2 = hasInterest(iab15, newI),
        _3 = hasInterest(iab16, newI),
        _4 = hasInterest(iab17, newI),
        _5 = hasInterest(iab18, newI),
        _6 = hasInterest(iab19, newI),
        _7 = hasInterest(iab20, newI),
        _8 = hasInterest(iab21, newI),
        _9 = hasInterest(iab22, newI),
        _10 = hasInterest(iab23, newI),
        _11 = hasInterest(iab24, newI),
        _12 = hasInterest(iab25, newI),
        _13 = hasInterest(iab26, newI))
      tuple13
    })

    val l1 = listInterests1.toDF("i1","i2","i3","i4","i5","i6", "i7","i8","i9","i10","i11","i12","i13")
    val l2 = listInterests2.toDF("i14","i15","i16","i17","i18","i19", "i20","i21","i22","i23","i24","i25","i26")
    (l1, l2)
  }

  /**
    * Return a DataFrame containing one column "size" cleaned.
    *
    * Different size values :
    * 0 -> null
    * 1 -> fullScreen / square
    * 2 -> horizontal
    * 3 -> vertical
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return a DataFrame
    */
  def cleanSize(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val size = data.select($"size")

    val sizeToStr = size.map(_.toString)

    val newSize = sizeToStr.map( value => {
      val a = value.toString.split(Array('(', ',' , ' ', ')'))
      a.size match {
        case 5 => {
          val l = a.apply(1)
          val h = a.apply(3)
          if ( (l, h) == ("300", "250") || (l, h) == ("200", "200") || (l, h) == ("250", "250") ||  (l, h) ==  ("336", "280") || (l, h) ==  ("480", "320")) 1
          else if(l.toInt > h.toInt) 2
          else 3
        }
        case _ => 0
      }
    })

    newSize.toDF("size")
  }

  /**
    * Return a DataFrame containing one column "city" cleaned.
    *
    * Different city values :
    * 1 -> non geo-located
    * 2 -> geo-located
    *
    * @param spark the active SparkSession
    * @param data the complete data
    * @return a DataFrame
    */
  def cleanCity(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val city = data.select($"city")

    val newCity = city.map( value => {
      value(0) match {
        case null => 1
        case _ => 2
      }
    })

    newCity.toDF("city")
  }

  /**
    * Return a DataFrame containing one column "type" cleaned.
    *
    * Different type values :
    * 0 -> null
    * 1 -> 0 or CLICK
    * 2 -> 1
    * 3 -> 2
    * 4 -> 3
    * 5 -> 4
    * @param spark the active SparkSession
    * @param data the complete data
    * @return a DataFrame
    */
  def cleanType(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val stype = data.select($"type")

    val newtype = stype.map(value => {
      value.toString match {
        case "[null]" => 0
        case "[0]" | "CLICK" => 1
        case "[1]" => 2
        case "[2]" => 3
        case "[3]" => 4
        case _ => 5
      }
    })
    newtype.toDF("type")
  }

}
