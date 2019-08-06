package org.globalforestwatch.features

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.Row
import org.globalforestwatch.util.GeometryReducer

object GadmFeature extends Feature {

  val countryPos = 1
  val adm1Pos = 2
  val adm2Pos = 3
  val geomPos = 7

  def getFeature(i: Row): geotrellis.vector.Feature[Geometry, GadmFeatureId] = {
    val countryCode: String = i.getString(countryPos)
    val admin1: String = i.getString(adm1Pos)
    val admin2: String = i.getString(adm2Pos)
    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )
    geotrellis.vector.Feature(geom, GadmFeatureId(countryCode, admin1, admin2))
  }

}
