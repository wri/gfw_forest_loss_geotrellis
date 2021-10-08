package org.globalforestwatch.util

import com.vividsolutions.jts.geom.{
  Geometry,
  GeometryCollection,
  MultiPolygon,
  Polygon,
  TopologyException,
}

import org.globalforestwatch.util.GeoSparkGeometryConstructor.createMultiPolygon

object IntersectGeometry {

  def intersectGeometries(thisGeom: Geometry,
                          thatGeom: Geometry): List[MultiPolygon] = {

    /**
      * Intersection can return GeometryCollections
      * Here we filter resulting geometries and only return those of the same type as thisGeom
      * */
    val userData = thisGeom.getUserData

    try {
      val intersection: Geometry = thisGeom intersection thatGeom
      intersection match {
        case poly: Polygon =>
          val multi = createMultiPolygon(Array(poly))
          multi.setUserData(userData)
          List(multi)
        case multi: MultiPolygon =>
          multi.setUserData(userData)
          List(multi)
        case collection: GeometryCollection =>
          val maybe_multi = extractPolygons(collection)
          maybe_multi match {
            case Some(multi) =>
              multi.setUserData(userData)
              List(multi)
            case _ => List()
          }
        case _ => List()
      }
    } catch {
      case e: TopologyException =>
        val wkt = new com.vividsolutions.jts.io.WKTWriter
        new java.io.PrintWriter("/tmp/thisGeom.wkt"){ write(wkt.write(thisGeom)); close }
        new java.io.PrintWriter("/tmp/thatGeom.wkt"){ write(wkt.write(thatGeom)); close }
        throw e
    }

  }

  def extractPolygons(multiGeometry: Geometry): Option[MultiPolygon] = {
    def loop(multiGeometry: Geometry): List[Polygon] = {

      val geomRange: List[Int] =
        List.range(0, multiGeometry.getNumGeometries)

      val nested_polygons = for {
        i <- geomRange

      } yield {
        multiGeometry.getGeometryN(i) match {
          case geom: Polygon => List(geom)
          case multiGeom: MultiPolygon => loop(multiGeom)
          case _ => List()
        }
      }
      nested_polygons.flatten
    }

    val polygons: List[Polygon] = loop(multiGeometry)

    if (polygons.isEmpty) None
    else Some(createMultiPolygon(polygons.toArray))

  }

}
