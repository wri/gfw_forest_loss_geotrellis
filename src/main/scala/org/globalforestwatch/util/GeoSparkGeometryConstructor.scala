package org.globalforestwatch.util

import com.vividsolutions.jts.geom.{Coordinate, CoordinateSequenceFactory, Geometry, GeometryFactory, MultiPolygon, Point, Polygon, PrecisionModel}
import com.vividsolutions.jts.io.WKBWriter
import org.globalforestwatch.util.Util.convertBytesToHex

object GeoSparkGeometryConstructor {
  val geomFactory = new GeometryFactory(new PrecisionModel(), 0)
  val coordSeqFactory: CoordinateSequenceFactory =
    geomFactory.getCoordinateSequenceFactory

  def createMultiPolygon(polygons: Array[Polygon],
                         srid: Int = 0): MultiPolygon = {

    val multiPolygon = new MultiPolygon(polygons, geomFactory)
    multiPolygon.setSRID(srid)
    multiPolygon
  }

  def createPolygon1x1(minX: Double, minY: Double, srid: Int = 0): Polygon = {

    val polygon = geomFactory.createPolygon(
      coordSeqFactory.create(
        Array(
          new Coordinate(minX, minY),
          new Coordinate(minX + 1, minY),
          new Coordinate(minX + 1, minY + 1),
          new Coordinate(minX, minY + 1),
          new Coordinate(minX, minY)
        )
      )
    )

    polygon.setSRID(srid)
    polygon
  }

  def createPoint(x: Double, y: Double, srid: Int = 0): Point = {

    val point = new Point(
      coordSeqFactory.create(Array(new Coordinate(x, y))),
      geomFactory
    )

    point.setSRID(srid)
    point
  }

  def toWKB(geom: Geometry): String = {
    val writer = new WKBWriter(2)
    convertBytesToHex(writer.write(geom))
  }
}
