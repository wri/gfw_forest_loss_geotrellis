package org.globalforestwatch.util

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKBWriter
import geotrellis.vector.{Geometry => GeotrellisGeometry}
import org.globalforestwatch.util.GeometryReducer.{makeValidGeom}
import org.globalforestwatch.util.Util.convertBytesToHex


object IntersectGeometry {


  def intersectGeometries(thisGeom: Geometry,
                          thatGeom: Geometry): List[Geometry] = {

    /**
      * Intersection can return GeometryCollections
      * Here we filter resulting geometries and only return those of the same type as thisGeom
      * */
    val intersection: Geometry = thisGeom intersection thatGeom

    intersection.getGeometryType match {
      case "GeometryCollection" => extractByGeometryType(intersection, thisGeom.getGeometryType)
      case geomType if thisGeom.getGeometryType contains geomType => List(intersection)
      case _ => List()
    }

  }

  def extractByGeometryType(geometryCollection: Geometry,
                            geometryType: String): List[Geometry] = {

    val geomRange: List[Int] =
      List.range(0, geometryCollection.getNumGeometries)

    for {
      i <- geomRange
      // GeometryType should be the same or a `non-Multi` type. Ie Polygon instead of MultiPolygon.
      if geometryType contains geometryCollection
        .getGeometryN(i)
        .getGeometryType
    } yield geometryCollection.getGeometryN(i)
  }

  def toGeotrellisGeometry(geom: Geometry): GeotrellisGeometry = {
    val writer = new WKBWriter(2)
    val wkb: String = convertBytesToHex(writer.write(geom))
    makeValidGeom(wkb)
  }
}
