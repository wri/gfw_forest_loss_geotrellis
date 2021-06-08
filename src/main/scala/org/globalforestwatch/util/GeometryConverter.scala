package org.globalforestwatch.util

import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry}
import geotrellis.vector.Geometry
import org.globalforestwatch.util.GeoSparkGeometryConstructor.toWKB
import org.globalforestwatch.util.GeotrellisGeometryValidator.makeValidGeom


object GeometryConverter {
  def toGeotrellisGeometry(geom: GeoSparkGeometry): Geometry = {
    val wkb = toWKB(geom)
    makeValidGeom(wkb)
  }
}
