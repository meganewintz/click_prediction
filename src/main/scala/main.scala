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
        myData.show()

        myData.createOrReplaceTempView("clicks")

        val appOrSite = spark.sql("SELECT appOrSite FROM clicks")

        val newAppOrsite = appOrSite.map( value => {
            value(0) match {
                case "app" => 0
                case "site" => 1
            }
        } )
        newAppOrsite.show()
//        val nbclass = appOrSite.filter(line => line.contains("app")).count()
//        val nbjavascript = appOrSite.filter(line => line.contains("javascript")).count()
//        println("Lignes avec javascript : %s, lignes avec class : %s".format(nbjavascript, nbclass))
        spark.stop()
    }


}