package org.globalforestwatch.features

import geotrellis.vector.Geometry
import org.apache.spark.sql.Row
import org.globalforestwatch.util.GeotrellisGeometryValidator.makeValidGeom

object GeostoreFeature extends Feature {

  val idPos = 0
  val geomPos = 1
  val featureCount = 1

  val featureIdExpr = "geostore_id as geostoreId"

  def get(
    i: Row
  ): geotrellis.vector.Feature[Geometry, FeatureId] = {
    val featureId = getFeatureId(i)
    val geom: Geometry = makeValidGeom(i.getString(geomPos))
    geotrellis.vector.Feature(geom, featureId)
  }

  def getFeatureId(i: Array[String], parsed: Boolean = false): FeatureId = {
    val geostoreId: String = i(idPos)
    GeostoreFeatureId(geostoreId)
  }

}
