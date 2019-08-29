package com.test

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ Param, ParamMap }
import org.apache.spark.ml.util.{ DefaultParamsReadable, DefaultParamsWritable, Identifiable }
import org.apache.spark.sql.{ DataFrame, Dataset }
import org.apache.spark.sql.types.StructType

class NoneDropTransformer(override val uid: String) extends Transformer with DefaultParamsWritable {
  val inputCols = new Param[Array[String]](this, "inputCols", "input columns")
  val how = new Param[String](this, "how", "how: any or all")

  def this() = this(Identifiable.randomUID("NoneDropTransformer"))
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)
  def getInputCols: Array[String] = getOrDefault(inputCols)
  def setHow(value: String): this.type = set(how, value)
  def getHow: String = getOrDefault(how)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val h = extractParamMap.getOrElse(how, "all")
    val inCols = extractParamMap.getOrElse(inputCols, Array("input"))

    dataset.na.drop(h, inCols)
  }

  override def copy(extra: ParamMap): NoneDropTransformer = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = schema
}

object NoneDropTransformer extends DefaultParamsReadable[NoneDropTransformer] {
  override def load(path: String): NoneDropTransformer = super.load(path)
}