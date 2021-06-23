package org.globalforestwatch.features

import geotrellis.vector.Geometry
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.globalforestwatch.util.GeotrellisGeometryValidator
import org.globalforestwatch.util.GeotrellisGeometryValidator.makeValidGeom
import org.globalforestwatch.util.Util.getAnyMapValue

trait Feature extends java.io.Serializable {
  val geomPos: Int
  val featureIdExpr: String

  def get(i: Row): geotrellis.vector.Feature[Geometry, FeatureId] = {
    val featureId = getFeatureId(i)
    val geom: Geometry = makeValidGeom(i.getString(geomPos))

    geotrellis.vector.Feature(geom, featureId)
  }

  def getFeatureId(i: Row): FeatureId = {
    getFeatureId(i.toSeq.map(_.asInstanceOf[String]).toArray)
  }

  def getFeatureId(i: Array[String], parsed: Boolean = false): FeatureId

  def isNonEmptyGeom(i: Row): Boolean = {
    GeotrellisGeometryValidator.isNonEmptyGeom(i.getString(geomPos))
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

    val gladDF =
      if (glad) customFilterDF.filter($"glad".isin(trueValues: _*))
      else customFilterDF

    val tclDF = if (tcl) gladDF.filter($"tcl".isin(trueValues: _*)) else gladDF

    limit.foldLeft(tclDF)(_.limit(_))

  }

  def custom_filter(filters: Map[String, Any])(df: DataFrame): DataFrame = {
    df
  }
}
