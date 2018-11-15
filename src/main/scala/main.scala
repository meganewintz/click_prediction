import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator


import Cleaner._

object ClickPrediction {
  def main(args: Array[String]) {

    val startTime = System.nanoTime()

    var filename = "Data.json"

    // Get the file enter in arg. By default the file is Data.json.
    if (args.length > 0) {
          filename = args(0)
    }

    // Configure SparkSession
    val spark = SparkSession
      .builder()
      .appName("Click prediction")
      .config("spark.master", "local")
      .getOrCreate();

    import spark.implicits._

    val dataset = spark.read.json(filename)

    // Clean the dataset
    val cleanDataset = cleanData(spark, dataset)


      // Split the data into training and test sets (30% held out for testing).
      val Array(trainingData, testData) = cleanDataset.randomSplit(Array(0.8, 0.2))

    val splits = cleanDataset.randomSplit(Array(0.8, 0.2))
    //val splits = cleanDataset.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val trainingDF = training.toDF()
    val test = splits(1)

    // Time when the Logisitc Regression begin.
    val startTimeLr = System.nanoTime()

    val lr = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.01)
        .setFamily("binomial")
        .setThreshold(0.5)

//  val rf = new RandomForestClassifier()
//      .setNumTrees(10)

    // Fit the model
    val lrModel = lr.fit(trainingData)

    // Time when the training is finish.
    val elapsedTimeLr1 = (System.nanoTime() - startTimeLr) / 1e9


    // run the  model on testData to get predictions
    val predictions = lrModel.transform(testData)

    //create an Evaluator for binary classification.
    val evaluator = new BinaryClassificationEvaluator()
        .setLabelCol("label")
        .setRawPredictionCol("rawPrediction")
        .setMetricName("areaUnderROC")

    //Evaluates predictions and returns a scalar metric.
    val accuracy = evaluator.evaluate(predictions)


    val lp = predictions.select( "label", "prediction")
    val countTotal = cleanDataset.count()
    val countTraining = trainingData.count()
    val countTest = predictions.toDF().count
    val labelTrue = lp.filter($"label" === 1.0).count()
    val labelFalse = lp.filter($"label" === 0.0).count()
    val predTrue = lp.filter($"prediction" === 1.0).count()
    val predFalse = lp.filter($"prediction" === 0.0).count()
    val correct = lp.filter($"label" === $"prediction").count()
    val wrong = lp.filter(not($"label" === $"prediction")).count()
    val trueP = lp.filter($"prediction" === 1.0).filter($"label" === $"prediction").count()
    val trueN = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count()
    val falseN = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
    val falseP = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count()
    val ratioWrong=wrong.toDouble/countTest.toDouble
    val ratioCorrect=correct.toDouble/countTest.toDouble

    val click = testData.filter("label = true").count()
    val goodClick = trueP.toDouble/click.toDouble
    val goodNoClick = trueN.toDouble/(countTest-click.toDouble)

      // Time when the test is finish.
      val elapsedTimeLr2 = (System.nanoTime() - startTimeLr) / 1e9

    val outputPrediction = revertLabel(spark, lp)

    outputPrediction
      .coalesce(1)
      .write.option("header","true")
      .mode("overwrite")
      .csv("prediction")

    val elapsedTime = (System.nanoTime() - startTime) / 1e9


    // ----------------------------------------------
    //              Print the results
    // ----------------------------------------------
    //lp.show()
    println("cleanData size : " + countTotal)
    println("training size : " + countTraining)
    println("accuracy : " + accuracy)
    println("ratioCorrect (good classification): " + ratioCorrect)
    println("ratioWrong (wrong classification): " + ratioWrong)
    println("good click : " + goodClick)
    println("good no click : " + goodNoClick)
    println("test size : " + countTest)
    println("correct : " + correct)
    println("wrong : " + wrong)
    println("prediction true : " + predTrue)
    println("prediction false : " + predFalse)
    println("label true : " + labelTrue)
    println("label false : " + labelFalse)
    println("trueP (good classification click): " + trueP)
    println("trueN (good classification no click: " + trueN)
    println("falseP : " + falseP)
    println("falseN : " + falseN)

    println(s"Training time: $elapsedTimeLr1 seconds")
    println(s"Training + prediction time: $elapsedTimeLr2 seconds")
    println(s"Total time: $elapsedTime seconds")
//
    // Print the coefficients and intercept for logistic regression
    println('-----------------------------------------------------------------------)
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    println('-----------------------------------------------------------------------)


    spark.stop()
  }



}