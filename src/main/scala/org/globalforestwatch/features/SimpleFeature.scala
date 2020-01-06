package org.globalforestwatch.features

import geotrellis.vector.{Feature, Geometry}
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.Row
import org.globalforestwatch.util.GeometryReducer

object SimpleFeature extends java.io.Serializable {

  val idPos = 0
  val geomPos = 1

  def getFeature(i: Row): Feature[Geometry, SimpleFeatureId] = {
    val feature_id: Int = i.getString(idPos).toInt
    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )
    Feature(geom, SimpleFeatureId(feature_id))
  }

  def isValidGeom(i: Row): Boolean = {
    GeometryReducer.isValidGeom(i.getString(geomPos))
  }

}
