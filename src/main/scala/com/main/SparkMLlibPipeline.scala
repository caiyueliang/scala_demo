package com.main

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassifier, XGBoostClassificationModel}

// this example works with Iris dataset (https://archive.ics.uci.edu/ml/datasets/iris)

object SparkMLlibPipeline {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Usage: SparkMLlibPipeline input_path native_model_path pipeline_model_path")
      sys.exit(1)
    }

    val inputPath = args(0)
    val nativeModelPath = args(1)
    val pipelineModelPath = args(2)

    println("[SparkMLlibPipeline] start ==========================================")
    println("        [inputPath] ", inputPath)
    println("  [nativeModelPath] ", nativeModelPath)
    println("[pipelineModelPath] ", pipelineModelPath)

    val spark = SparkSession.builder().appName("XGBoost_Test").getOrCreate()

    // Load dataset
    val schema = new StructType(Array(
      StructField("unique_id", IntegerType, false),
      StructField("sample_day", StringType, false),
      StructField("tag", IntegerType, false),
      StructField("gender", IntegerType, false),
      StructField("age_g", IntegerType, false),
      StructField("city", IntegerType, false),
      StructField("zmscore_g", IntegerType, false),
      StructField("bscore_g", IntegerType, false),
      StructField("td_score_g", IntegerType, false),
      StructField("ivs3_score_g", IntegerType, false),
      StructField("is_overdue_user", IntegerType, false),
      StructField("is_overdue", IntegerType, false)
    ))

    var rawInput = spark.read.schema(schema).option("header", true).option("sep", "\t").csv(inputPath)
    rawInput.show()
    rawInput = rawInput.na.drop()
    rawInput.show()

    // Split training and test dataset
    val Array(training, test) = rawInput.randomSplit(Array(0.8, 0.2), 123)

    // Build ML pipeline, it includes 4 stages:
    // 1, Assemble all features into a single vector column.
    // 2, From string label to indexed double label.
    // 3, Use XGBoostClassifier to train classification model.
    // 4, Convert indexed double label back to original string label.
    val assembler = new VectorAssembler()
      .setInputCols(Array("tag", "gender", "age_g", "city", "zmscore_g", "bscore_g", "td_score_g", "ivs3_score_g", "is_overdue_user"))
      .setOutputCol("features")

    // val labelIndexer = new StringIndexer()
    //   .setInputCol("class")
    //   .setOutputCol("classIndex")
    //   .fit(training)

    val booster = new XGBoostClassifier()
    booster.setMaxDepth(3)
    booster.setNumClass(2)
    booster.setFeaturesCol("features")
    booster.setLabelCol("is_overdue")
    booster.setPredictionCol("predictionCol")

    // val labelConverter = new IndexToString()
    //   .setInputCol("prediction")
    //   .setOutputCol("realLabel")
    //   .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(assembler, booster))
    println("[SparkMLlibPipeline] train start ==========================================")
    val model = pipeline.fit(training)

    // Batch prediction
    val prediction = model.transform(test)
    prediction.show(false)

    // Model evaluation
    val evaluator = new MulticlassClassificationEvaluator()
    evaluator.setLabelCol("is_overdue")
    evaluator.setPredictionCol("prediction")
    val accuracy = evaluator.evaluate(prediction)
    println("The model accuracy is : " + accuracy)

    // Tune model using cross validation
    val paramGrid = new ParamGridBuilder()
      .addGrid(booster.maxDepth, Array(3, 8))
      .addGrid(booster.eta, Array(0.2, 0.6))
      .build()
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val cvModel = cv.fit(training)

    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[XGBoostClassificationModel]
    println("The params of best XGBoostClassification model : " + bestModel.extractParamMap())
    println("The training summary of best XGBoostClassificationModel : " + bestModel.summary)

    println("[SparkMLlibPipeline] save model ==========================================")
    // Export the XGBoostClassificationModel as local XGBoost model,
    // then you can load it back in local Python environment.
    bestModel.nativeBooster.saveModel(nativeModelPath)

    // ML pipeline persistence
    model.write.overwrite().save(pipelineModelPath)

    // Load a saved model and serving
    val model2 = PipelineModel.load(pipelineModelPath)
    model2.transform(test).show(false)
  }
}

