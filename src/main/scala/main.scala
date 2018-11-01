/* SimpleProg.scala */
import org.apache.spark.sql.SparkSession

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.RFormula


import Cleaner._


object SimpleProg {
  def main(args: Array[String]) {
    val myFile = "data-students.json"
    val spark = SparkSession
      .builder()
      .appName("Click prediction")
      .config("spark.master", "local")
      .getOrCreate();

    import spark.implicits._

    val dataset = spark.read.json(myFile)

    val cleanDataset = cleanData(spark, dataset)
    //cleanDataset

    
//    val lr = new LogisticRegression()
//        .setMaxIter(10)
//        .setRegParam(0.3)
//        .setElasticNetParam(0.8)
//
//    // Fit the model
//    val lrModel = lr.fit(cleanDataset)
//
//    // Print the coefficients and intercept for logistic regression
//    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
//
    // We can also use the multinomial family for binary classification
//    val mlr = new LogisticRegression()
//        .setMaxIter(10)
//        .setRegParam(0.3)
//        .setElasticNetParam(0.8)
//        .setFamily("binomial")
//
//    val mlrModel = mlr.fit(cleanDataset)
//
//    // Print the coefficients and intercepts for logistic regression with multinomial family
//    println(s"Binomial coefficients: ${mlrModel.coefficientMatrix}")
//    println(s"Binamial intercepts: ${mlrModel.interceptVector}")



    spark.stop()
  }


}