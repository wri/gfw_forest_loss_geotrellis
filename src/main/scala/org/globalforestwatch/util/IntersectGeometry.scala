package org.globalforestwatch.util

import cats.data.Validated.{Valid, Invalid}
import org.locationtech.jts.geom.{
  Geometry,
  GeometryCollection,
  MultiPolygon,
  Polygon
}
import scala.util.{Try, Success, Failure}

import org.globalforestwatch.util.GeometryConstructor.createMultiPolygon
import org.globalforestwatch.summarystats.{GeometryError, ValidatedRow}

object IntersectGeometry {

  def intersectGeometries(thisGeom: Geometry,
                          thatGeom: Geometry): List[MultiPolygon] = {

    /**
      * Intersection can return GeometryCollections
      * Here we filter resulting geometries and only return those of the same type as thisGeom
      * */
    val userData = thisGeom.getUserData

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
          extractPolygons(collection)
            .filterNot(_.isEmpty)
            .map{ geom =>
              geom.setUserData(userData)
              geom
            }.toList
      case _ => List()
    }
  }

  def validatedIntersection(
    thisGeom: Geometry,
    thatGeom: Geometry
  ): ValidatedRow[List[MultiPolygon]] = {
    /**
      * Intersection can return GeometryCollections
      * Here we filter resulting geometries and only return those of the same type as thisGeom
      * */
    val userData = thisGeom.getUserData

    val attempt = Try {
      val intersection: Geometry = thisGeom intersection thatGeom
      intersection match {
        case poly: Polygon =>
          val multi = createMultiPolygon(Array(poly))
          multi.setUserData(userData)
          List(multi).filterNot(_.isEmpty)
        case multi: MultiPolygon =>
          multi.setUserData(userData)
          List(multi).filterNot(_.isEmpty)
        case collection: GeometryCollection =>
          extractPolygons(collection)
            .filterNot(_.isEmpty)
            .map{ geom =>
              geom.setUserData(userData)
              geom
            }.toList
        case _ => List()
      }
    }

    attempt match {
      case Success(v) => Valid(v)
      case Failure(e) => Invalid(GeometryError(s"Failed intersection with ${thatGeom}"))
    }
  }

  def extractPolygonsToList(multiGeometry: Geometry): List[Polygon] = {
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

    loop(multiGeometry)
  }

  def extractPolygons(multiGeometry: Geometry): Option[MultiPolygon] = {
    val polygons = extractPolygonsToList(multiGeometry)
    if (polygons.isEmpty) None
    else Some(createMultiPolygon(polygons.toArray))
  }

}
