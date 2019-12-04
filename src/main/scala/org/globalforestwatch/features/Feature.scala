package org.globalforestwatch.features

import geotrellis.vector.Geometry
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.globalforestwatch.util.GeometryReducer
import org.globalforestwatch.util.Util.getAnyMapValue

trait Feature extends java.io.Serializable {

  val geomPos: Int

  def getFeature(i: Row): geotrellis.vector.Feature[Geometry, FeatureId]

  def isValidGeom(i: Row): Boolean = {
    GeometryReducer.isValidGeom(i.getString(geomPos))
  }

  def filter(filters: Map[String, Any])(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    var newDF = df
    newDF.transform(custom_filter(filters))

    val limit: Option[Int] = getAnyMapValue[Option[Int]](filters, "limit")
    val tcl: Boolean = getAnyMapValue[Boolean](filters, "tcl")
    val glad: Boolean = getAnyMapValue[Boolean](filters, "glad")

    val trueValues: List[String] =
      List("t", "T", "true", "True", "TRUE", "1", "Yes", "yes", "YES")

    if (glad) newDF = newDF.filter($"glad".isin(trueValues: _*))

    if (tcl) newDF = newDF.filter($"tcl".isin(trueValues: _*))

    limit.foreach { n =>
      newDF = newDF.limit(n)
    }

    newDF
  }

  def custom_filter(filters: Map[String, Any])(df: DataFrame): DataFrame = {
    df
  }
}
