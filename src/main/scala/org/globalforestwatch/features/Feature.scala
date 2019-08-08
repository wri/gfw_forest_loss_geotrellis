package org.globalforestwatch.features

import geotrellis.vector.Geometry
import org.apache.spark.sql.{DataFrame, Row}
import org.globalforestwatch.util.GeometryReducer

trait Feature extends java.io.Serializable {

  val geomPos: Int

  def getFeature(i: Row): geotrellis.vector.Feature[Geometry, FeatureId]

  def isValidGeom(i: Row): Boolean = {
    GeometryReducer.isValidGeom(i.getString(geomPos))

  }

  def filter(filters: Map[String, Any])(df: DataFrame): DataFrame

  def getMapValue[T: Manifest](map: Map[String, Any], key: String): T =
    map(key) match {
      case v: T => v
      case _ => throw new IllegalArgumentException("Wrong type")
    }
}
