package org.globalforestwatch.util

import geotrellis.vector.{Geometry, Polygon, Point}
import org.locationtech.jts.geom.Coordinate

object IntersectGeometry {

  def createPolygon(minX: Double, minY: Double): Polygon = {
    val polygon: Polygon = Polygon(
      Point(minX, minY),
      Point(minX + 1, minY),
      Point(minX + 1, minY + 1),
      Point(minX, minY + 1),
      Point(minX, minY)
    )
    polygon.setSRID(4326)
    polygon
  }

  def getIntersecting1x1Grid(geometry: Geometry): IndexedSeq[Polygon] = {

    /**
      * Return grid of 1x1 degree polygons intersecting with input geometry
      * */
    val coords: Array[Coordinate] = geometry.getEnvelope.getCoordinates
    val (minX, minY, maxX, maxY) = coords.foldLeft(180.0, 90.0, -180.0, -90.0)(
      (z, coord) =>
        (coord.x min z._1, coord.y min z._2, coord.x max z._3, coord.y max z._4)
    )

    for {
      x <- minX.floor.toInt until maxX.ceil.toInt
      y <- minY.floor.toInt until maxY.ceil.toInt
      if {
        val polygon = createPolygon(x, y)
        polygon.within(TreeCoverLossExtent.geometry) && polygon.intersects(
          geometry
        )
      }

    } yield {
      createPolygon(x, y)

    }

  }

  def intersectGeometries(thisGeom: Geometry,
                          thatGeom: Geometry): List[Geometry] = {

    /**
      * Intersection can return GeometryCollections
      * Here we filter resulting geometries and only return those of the same type as thisGeom
      * This will also explode MultiPolygons into separate features.
      * */
    val intersection: Geometry = thisGeom intersection thatGeom

    val geomRange: List[Int] = List.range(0, intersection.getNumGeometries)

    for {
      i <- geomRange
      // GeometryType should be the same or a `non-Multi` type. Ie Polygon instead of MultiPolygon.
      if thisGeom.getGeometryType contains intersection
        .getGeometryN(i)
        .getGeometryType
    } yield {
      val geom = intersection.getGeometryN(i)
      val normalizedGeom = {
        // try to make geometry valid. This is a basic trick, we might need to make this more sophisticated
        // There are some code samples here for JTS
        // https://stackoverflow.com/a/31474580/1410317
        if (!geom.isValid) geom.buffer(0.0001).buffer(-0.0001)
        else geom
      }

      normalizedGeom.normalize()
      normalizedGeom
    }

  }
}
