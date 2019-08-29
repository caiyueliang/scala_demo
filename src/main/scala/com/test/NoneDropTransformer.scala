package com.test

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ Param, ParamMap }
import org.apache.spark.ml.util.{ DefaultParamsReadable, DefaultParamsWritable, Identifiable }
import org.apache.spark.sql.{ DataFrame, Dataset }
import org.apache.spark.sql.types.StructType

class NoneDropTransformer(override val uid: String) extends Transformer with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("NoneDropTransformer"))
  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def getOutputCol: String = getOrDefault(outputCol)

  val inputCol = new Param[String](this, "inputCol", "input column")
  val outputCol = new Param[String](this, "outputCol", "output column")

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outCol = extractParamMap.getOrElse(outputCol, "output")
    val inCol = extractParamMap.getOrElse(inputCol, "input")

    dataset.drop(outCol).withColumnRenamed(inCol, outCol)
  }

  override def copy(extra: ParamMap): ColRenameTransformer = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = schema
}

object NoneDropTransformer extends DefaultParamsReadable[NoneDropTransformer] {
  override def load(path: String): NoneDropTransformer = super.load(path)
}