package org.globalforestwatch.features

import geotrellis.vector.Geometry
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.globalforestwatch.util.GeotrellisGeometryValidator
import org.globalforestwatch.util.GeotrellisGeometryValidator.makeValidGeom
import geotrellis.raster.summary.polygonal.Summary

/** This trait defiens how to read a Feature from DataFrame, from what columns to parse its FeatureId and how to read its geometry */
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
    try {
      GeotrellisGeometryValidator.isNonEmptyGeom(i.getString(geomPos))
    } catch {
      case e: Exception =>
        println(s"Unable to process geometry for row: ${i}")
        throw e
    }
  }

  def filter(filters: FeatureFilter)(df: DataFrame): DataFrame = {
    val conditions = filters.filterConditions()
    if (conditions.isEmpty) df else {
      val condition = conditions.reduce(_ and _)
      df.filter(condition)
    }
  }
}

object Feature {
  def apply(name: String): Feature = name match {
    case "gadm" => GadmFeature
    case "feature" => SimpleFeature
    case "wdpa" => WdpaFeature
    case "geostore" => GeostoreFeature
    case "viirs" => FireAlertViirsFeature
    case "modis" => FireAlertModisFeature
    case "burned_areas" => BurnedAreasFeature
    case "gfwpro" => GfwProFeature
    case value =>
      throw new IllegalArgumentException(
        s"FeatureType must be one of 'gadm', 'wdpa', 'geostore', 'gfwpro', 'feature', 'viirs', 'modis', or 'burned_areas'. Got $value."
      )
  }
}