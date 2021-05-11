package org.globalforestwatch.features

import geotrellis.vector.Geometry
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.globalforestwatch.util.GeometryReducer
import org.globalforestwatch.util.Util.getAnyMapValue

trait Feature extends java.io.Serializable {
  val geomPos: Int
  val featureCount: Int
  val featureIdExpr: String

  def get(i: Row): geotrellis.vector.Feature[Geometry, FeatureId]

  def getFeatureId(i: Row): FeatureId = {
    getFeatureId(i.toSeq.map(_.asInstanceOf[String]).toArray)
  }

  def getFeatureId(i: Array[String]): FeatureId

  def isValidGeom(i: Row): Boolean = {
    GeometryReducer.isValidGeom(i.getString(geomPos))
  }

  def filter(filters: Map[String, Any])(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val trueValues: List[String] =
      List("t", "T", "true", "True", "TRUE", "1", "Yes", "yes", "YES")
    val limit: Option[Int] = getAnyMapValue[Option[Int]](filters, "limit")
    val tcl: Boolean = getAnyMapValue[Boolean](filters, "tcl")
    val glad: Boolean = getAnyMapValue[Boolean](filters, "glad")

    val customFilterDF = df.transform(custom_filter(filters))

    val gladDF = if (glad) customFilterDF.filter($"glad".isin(trueValues: _*)) else customFilterDF

    val tclDF = if (tcl) gladDF.filter($"tcl".isin(trueValues: _*)) else gladDF

    limit.foldLeft(tclDF)(_.limit(_))

  }

  def custom_filter(filters: Map[String, Any])(df: DataFrame): DataFrame = {
    df
  }
}
