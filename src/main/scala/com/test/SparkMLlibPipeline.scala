package com.test

import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassificationModel, XGBoostClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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
      StructField("unique_id", IntegerType, true),
      StructField("sample_day", StringType, true),
      StructField("tag", IntegerType, true),
      StructField("gender", IntegerType, true),
      StructField("age_g", IntegerType, true),
      StructField("city", IntegerType, true),
      StructField("zmscore_g", IntegerType, true),
      StructField("bscore_g", IntegerType, true),
      StructField("td_score_g", IntegerType, true),
      StructField("ivs3_score_g", IntegerType, true),
      StructField("is_overdue_user", IntegerType, true),
      StructField("is_overdue", IntegerType, true)
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

    val booster = new XGBoostClassifier(
      Map("eta" -> 0.1f,
        "max_depth" -> 5,
        "objective" -> "multi:softprob",
        "num_class" -> 2,
        "num_round" -> 100,
        "num_workers" -> 1
      )
    )
    booster.setFeaturesCol("features")
    booster.setLabelCol("is_overdue")
    // booster.setPredictionCol("predictionCol")

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
