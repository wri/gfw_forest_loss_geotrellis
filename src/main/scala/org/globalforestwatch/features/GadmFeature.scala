package org.globalforestwatch.features

import geotrellis.vector.{Feature, Geometry}
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.Row
import org.globalforestwatch.util.GeometryReducer

object GadmFeature extends java.io.Serializable {

  val countryPos = 1
  val adm1Pos = 2
  val adm2Pos = 3
  val geomPos = 4

  def getFeature(i: Row): Feature[Geometry, GadmFeatureId] = {
    val countryCode: String = i.getString(countryPos)
    val admin1: String = i.getString(adm1Pos)
    val admin2: String = i.getString(adm2Pos)
    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )
    SimpleFeature(geom, GadmFeatureId(countryCode, admin1, admin2))
  }

  def isValidGeom(i: Row): Boolean = {
    GeometryReducer.isValidGeom(i.getString(geomPos))
  }

}
