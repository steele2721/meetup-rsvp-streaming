package analysis

import java.io.IOException
import java.util.logging.Logger

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryException

object TrainingRSVPMongoDBStaticLoad {

  val logger: Logger = Logger.getLogger(TrainingRSVPMongoDBStaticLoad.getClass.getName)

  val CaseSensitive = "false"

  val ModelOutPutURI: String = "/home/robertsteele/GradleProjects/" +
    "StreamingApplication/analysis_tier/src/main/resources/" +
    "LogisticRegressionModels.model"
  val MongoInputDBURI = "mongodb://localhost:27017/meetupDB"
  val MongoInputCollectionName = "rsvps"


  @throws(classOf[InterruptedException])
  @throws(classOf[StreamingQueryException])
  @throws(classOf[IOException])
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("TrainingRSVPMongoDBLoad")
      .set("spark.sql.caseSensitive", CaseSensitive)
      .set("spark.mongodb.input.uri", MongoInputDBURI)

    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.mongodb.input.collection", MongoInputCollectionName)
      .config("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.numberOfPartitions", "1")
      .config("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
      .getOrCreate()

    val meetupRSVPsDF = MongoSpark.loadAndInferSchema(spark)

    logger.info("----------------------------")
    logger.info("MongoDB collection loaded")
    logger.info(meetupRSVPsDF.take(10).mkString(""))
    logger.info("----------------------------")

    import spark.implicits._

    val selectedColumnsForFeaturesDF = meetupRSVPsDF
      .select($"group.group_city",
        $"group.group_lat",
        $"group.group_lon",
        $"response")

    val filteredDataDF = selectedColumnsForFeaturesDF.filter(row => !row.anyNull)

    val labeledDF = filteredDataDF.withColumn("response",
      when($"response" === "no", 0)
        .when($"response" === "yes", 1)
        .otherwise(1))

    val featuresAsColumnsWithLabelsDF = labeledDF
      .withColumn("binary_response", $"response".cast("double"))
      .drop("response")
      .withColumnRenamed("binary_response", "label")

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("group_lat", "group_lon"))
      .setOutputCol("features")

    val logisticRegressionModel = new LogisticRegression()
      .setMaxIter(1000)
      .setRegParam(0.00001)
      .setElasticNetParam(0.1)
      .setThreshold(0.1)

    val pipeline = new Pipeline()
      .setStages(Array(vectorAssembler, logisticRegressionModel))

    val splitDataFrame = featuresAsColumnsWithLabelsDF.randomSplit(Array(.8, .2))
    val trainDF = splitDataFrame(0)
    trainDF.show()
    val evaluationDF = splitDataFrame(1)

    val pipelineModel = pipeline.fit(trainDF)

    val predictionsDF = pipelineModel.transform(evaluationDF)

    predictionsDF.show(false)

    val binaryClassificationEvaluator = new BinaryClassificationEvaluator()

    val metricName = binaryClassificationEvaluator.getMetricName

    val largerBetter = binaryClassificationEvaluator.isLargerBetter

    val evalValue = binaryClassificationEvaluator.evaluate(predictionsDF)

    logger.info("\n------------------\n" +
      "Binary Classification Evaluator: \nMetric Name: " +
      metricName + "\nIs larger better?: " + largerBetter + "\nValue: " + evalValue +
      "\n------------------\n")

    pipelineModel.save(ModelOutPutURI)
  }


}
