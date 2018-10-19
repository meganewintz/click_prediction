/* SimpleProg.scala */
import org.apache.spark.sql.SparkSession


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
        myData.printSchema()
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
        //newAppOrsite.show()

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
        exchange.show()

        val newExchange = label.map( value => {
            if (value(0).toString.contains("f8dd")) 0
            else if (value(0).toString.contains("c7a327")) 1
            else if (value(0).toString.contains("46135")) 2
            else  3
        } )
        newExchange.show()




//        val nbclass = appOrSite.filter(line => line.contains("app")).count()
//        val nbjavascript = appOrSite.filter(line => line.contains("javascript")).count()
//        println("Lignes avec javascript : %s, lignes avec class : %s".format(nbjavascript, nbclass))
        spark.stop()
    }


}