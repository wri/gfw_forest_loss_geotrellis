package org.globalforestwatch.features

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.Row
import org.globalforestwatch.util.GeometryReducer

object SimpleFeature extends Feature {

  val idPos = 0
  val geomPos = 1

  def getFeature(i: Row): geotrellis.vector.Feature[Geometry, SimpleFeatureId] = {
    val feature_id: Int = i.getString(idPos).toInt
    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )
    geotrellis.vector.Feature(geom, SimpleFeatureId(feature_id))
  }

}
