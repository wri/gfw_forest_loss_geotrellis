package org.globalforestwatch.util

import com.vividsolutions.jts.geom.{
  Geometry,
  GeometryCollection,
  MultiPolygon,
  Polygon,
}

import org.globalforestwatch.util.GeoSparkGeometryConstructor.createMultiPolygon

object IntersectGeometry {

  def intersectGeometries(thisGeom: Geometry,
                          thatGeom: Geometry): List[MultiPolygon] = {

    /**
      * Intersection can return GeometryCollections
      * Here we filter resulting geometries and only return those of the same type as thisGeom
      * */
    val userData = thisGeom.getUserData.asInstanceOf[String]

    val intersection: Geometry = thisGeom intersection thatGeom
    intersection match {
      case collection: GeometryCollection =>
        val multi = extractPolygons(collection)
        multi.setUserData(userData)
        List(multi)
      case multi: MultiPolygon =>
        multi.setUserData(userData)
        List(multi)
      case poly: Polygon =>
        val multi = createMultiPolygon(Array(poly))
        multi.setUserData(userData)
        List(multi)
      case _ => List()
    }

  }

  def extractPolygons(multiGeometry: Geometry): MultiPolygon = {
    def loop(multiGeometry: Geometry): List[Polygon] = {

      val geomRange: List[Int] =
        List.range(0, multiGeometry.getNumGeometries)

      val nested_polygons = for {
        i <- geomRange

      } yield {
        multiGeometry.getGeometryN(i) match {
          case geom: Polygon => List(geom)
          case multiGeom: MultiPolygon => loop(multiGeom)
        }
      }
      nested_polygons.flatten
    }

    val polygons: List[Polygon] = loop(multiGeometry)
    createMultiPolygon(polygons.toArray)

  }

}
