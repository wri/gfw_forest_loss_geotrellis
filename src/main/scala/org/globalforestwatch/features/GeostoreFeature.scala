package org.globalforestwatch.features

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.Row
import org.globalforestwatch.util.GeometryReducer

object GeostoreFeature extends Feature {

  val idPos = 0
  val geomPos = 1

  val featureIdExpr = "geostore_id as geostoreId"

  def get(
    i: Row
  ): geotrellis.vector.Feature[Geometry, FeatureId] = {
    val featureId = getFeatureId(i)
    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )
    geotrellis.vector.Feature(geom, featureId)
  }

  def getFeatureId(i: Array[String]): FeatureId = {
    val geostoreId: String = i(idPos)
    GeostoreFeatureId(geostoreId)
  }

}
