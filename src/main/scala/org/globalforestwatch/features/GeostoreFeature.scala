package org.globalforestwatch.features

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.Row
import org.globalforestwatch.util.GeometryReducer

object GeostoreFeature extends Feature {

  val idPos = 0
  val geomPos = 1

  def getFeature(
    i: Row
  ): geotrellis.vector.Feature[Geometry, GeostoreFeatureId] = {
    val geostore_id: String = i.getString(idPos)
    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )
    geotrellis.vector.Feature(geom, GeostoreFeatureId(geostore_id))
  }

}
