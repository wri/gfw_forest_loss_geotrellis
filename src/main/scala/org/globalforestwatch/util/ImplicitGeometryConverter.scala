package org.globalforestwatch.util

import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry}
import com.vividsolutions.jts.io.WKBReader
import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.globalforestwatch.util.GeoSparkGeometryConstructor.toWKB
import org.globalforestwatch.util.GeotrellisGeometryValidator.makeValidGeom
//import org.locationtech.jts.geom.{Geometry => LocationTechGeometry}
//import scala.language.implicitConversions

object ImplicitGeometryConverter {
  implicit def toGeotrellisGeometry[I <: GeoSparkGeometry, O <: Geometry](
                                                                           geom: I
                                                                         ): O = {
    val wkb = toWKB(geom)
    makeValidGeom(wkb).asInstanceOf[O]
  }

  implicit def toGeoSparkGeometry[I <: Geometry, O <: GeoSparkGeometry](
                                                                         geom: I
                                                                       ): O = {
    val wkb: Array[Byte] = WKB.write(geom)
    val reader = new WKBReader()
    reader.read(wkb).asInstanceOf[O]
  }

  //  implicit def toGeoSparkGeometry2[I <: LocationTechGeometry, O<: GeoSparkGeometry](
  //                                                                                     geom: I
  //                                                                                   ): O = {
  //    val wkb: Array[Byte] = WKB.write(geom)
  //    val reader = new WKBReader()
  //    reader.read(wkb).asInstanceOf[O]
  //  }

}
