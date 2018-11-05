/* SimpleProg.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.{RFormula, VectorAssembler}
import org.apache.spark.sql.functions._
import Cleaner._
import breeze.linalg.max
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator


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
    //cleanData(spark, dataset)
    //cleanDataset

    //val featuresCols = Array("appOrSite", "os", "exchange", "media", "publisher", "i1", "i2","i3", "i4", "i5", "i6", "i7","i8","i10","i11","i12", "i13", "i14", "i15", "i16", "i17", "i18", "i19", "i20", "i21", "i22", "i23", "i24", "i25", "i26", "size", "city", "type")
    val featuresCols = Array("appOrSite", "os", "exchange", "media", "publisher", "size", "city", "type")
    val assembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("features")
    val df2 = assembler.transform(cleanDataset.toDF())

    val splits = df2.randomSplit(Array(0.8, 0.2), seed=11L)
    val training = splits(0).cache()
    val trainingDF = training.toDF()
    val test = splits(1)

   // df2.show(false)

    val lr = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.3)
        //.setElasticNetParam(0.8)


    // Fit the model
    val lrModel = lr.fit(trainingDF)

    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
    // example
    val trainingSummary = lrModel.binarySummary

    // Obtain the objective per iteration.
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

    // run the  model on test features to get predictions**
    val predictions = lrModel.transform(test)
    //As you can see, the previous model transform produced a new columns: rawPrediction, probablity and prediction.**
    predictions.show(false)

    // create an Evaluator for binary classification, which expects two input columns: rawPrediction and label.**
    //val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
    // Evaluates predictions and returns a scalar metric areaUnderROC(larger is better).**
    //val accuracy = evaluator.evaluate(predictions)
    //println("accuracy : " + accuracy)

    val lp = predictions.select( "label", "prediction")
    lp.show()
    println("---- class ---- " + lp.getClass)
    /*val counttotal = predictions.toDF().count
    val correct = lp.filter($"label" === $"prediction").count()
    val wrong = lp.filter(not($"label" === $"prediction")).count()
    val truep = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count()
    val falseN = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
    val falseP = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count()*/
    //val ratioWrong=wrong.toDouble/counttotal.toDouble
   // val ratioCorrect=correct.toDouble/counttotal.toDouble

   /* println("correct : " + correct)
    println("wrong : " + wrong)
    println("trueP : " + truep)
    println("falseN : " + falseN)
    println("falseP : " + falseP)
    println("falseN : " + falseN)*/
    //println("ratioWrong : " + ratioWrong)
    //println("ratioCorrect : " + ratioCorrect)

    // Print the coefficients and intercept for logistic regression
    /*println('-----------------------------------------------------------------------)
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    println('-----------------------------------------------------------------------)

    // Print the coefficients and intercepts for logistic regression with multinomial family
    println(s"Binomial coefficients: ${lrModel.coefficientMatrix}")
    println(s"Binamial intercepts: ${lrModel.interceptVector}")*/



    spark.stop()
  }


}