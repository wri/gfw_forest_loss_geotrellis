package org.globalforestwatch.util

import com.vividsolutions.jts.geom.{
  Geometry,
  GeometryCollection,
  GeometryFactory,
  LineString,
  MultiLineString,
  MultiPoint,
  MultiPolygon,
  Point,
  Polygon,
  PrecisionModel
}
import com.vividsolutions.jts.io.WKBWriter
import geotrellis.vector.{Geometry => GeotrellisGeometry}
import org.globalforestwatch.util.GeometryReducer.makeValidGeom
import org.globalforestwatch.util.Util.convertBytesToHex

object IntersectGeometry {

  def intersectGeometries(thisGeom: Geometry,
                          thatGeom: Geometry): List[Polygon] = {

    /**
      * Intersection can return GeometryCollections
      * Here we filter resulting geometries and only return those of the same type as thisGeom
      * */
    val userData = thisGeom.getUserData.asInstanceOf[String]

    val intersection: Geometry = thisGeom intersection thatGeom
    intersection match {
      case _: GeometryCollection | _: MultiPolygon =>
        extractPolygons(intersection).map { geom =>
          geom.setUserData(userData)
          geom
        }
      case geom: Polygon =>
        geom.setUserData(userData)
        List(geom)

      case _ => List()
    }

  }

  //  def extractByGeometryType(geometryCollection: Geometry,
  //                            geometryType: String): List[Geometry] = {
  //
  //    val geomRange: List[Int] =
  //      List.range(0, geometryCollection.getNumGeometries)
  //
  //    for {
  //      i <- geomRange
  //      // GeometryType should be the same or a `non-Multi` type. Ie Polygon instead of MultiPolygon.
  //      if geometryType contains geometryCollection
  //        .getGeometryN(i)
  //        .getGeometryType
  //    } yield geometryCollection.getGeometryN(i)
  //  }

  //  def toMultiGeometry(geometry:Geometry): Geometry = {
  //    val factory = new GeometryFactory(new PrecisionModel(), 4326)
  //
  //    geometry match {
  //      case point: Point => new MultiPoint(Array(point), factory)
  //      case line: LineString => new MultiLineString(Array(line), factory)
  //      case polygon: Polygon => new MultiPolygon(Array(polygon), factory)
  //      case geom => geom
  //
  //    }
  //  }

  def extractPolygons(multiGeometry: Geometry): List[Polygon] = {

    val geomRange: List[Int] =
      List.range(0, multiGeometry.getNumGeometries)

    val result = for {
      i <- geomRange

    } yield {
      multiGeometry.getGeometryN(i) match {
        case geom: Polygon => List(geom)
        case multiGeom: MultiPolygon => extractPolygons(multiGeom)
      }
    }
    result.flatten
  }

  def toGeotrellisGeometry(geom: Geometry): GeotrellisGeometry = {
    val writer = new WKBWriter(2)
    val wkb: String = convertBytesToHex(writer.write(geom))
    makeValidGeom(wkb)
  }
}
