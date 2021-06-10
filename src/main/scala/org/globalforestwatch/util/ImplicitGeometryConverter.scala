package org.globalforestwatch.util

import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry}
import com.vividsolutions.jts.io.WKBReader
import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.globalforestwatch.util.GeoSparkGeometryConstructor.toWKB
import org.globalforestwatch.util.GeotrellisGeometryValidator.makeValidGeom

object ImplicitGeometryConverter {
  implicit def toGeotrellisGeometry[I <: GeoSparkGeometry, O <: Geometry](
                                                                           geom: I
                                                                         ): O = {
    val wkb = toWKB(geom)
    val newGeom = makeValidGeom(wkb).asInstanceOf[O]
    newGeom.setUserData(geom.getUserData)
    newGeom.setSRID(geom.getSRID)
    newGeom
  }

  implicit def toGeoSparkGeometry[I <: Geometry, O <: GeoSparkGeometry](
                                                                         geom: I
                                                                       ): O = {
    val wkb: Array[Byte] = WKB.write(geom)
    val reader = new WKBReader()
    val newGeom = reader.read(wkb).asInstanceOf[O]
    newGeom.setUserData(geom.getUserData)
    newGeom.setSRID(geom.getSRID)
    newGeom
  }

}
