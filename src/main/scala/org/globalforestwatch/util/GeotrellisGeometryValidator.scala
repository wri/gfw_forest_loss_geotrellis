package org.globalforestwatch.util

import geotrellis.vector.{
  Geometry,
  LineString,
  MultiPoint,
  Point,
  Polygon,
  MultiLineString,
  MultiPolygon
}
import geotrellis.vector.io.wkb.WKB
import org.globalforestwatch.util.GeotrellisGeometryReducer.{gpr, reduce}

object GeotrellisGeometryValidator extends java.io.Serializable {

  def isNonEmptyGeom(geom: Geometry): Boolean = {
    val maybeGeom: Option[Geometry] = try {
      Some(reduce(gpr)(geom))
    } catch {
      case ae: java.lang.AssertionError =>
        println("There was an empty geometry")
        None
      case t: Throwable => throw t
    }

    maybeGeom match {
      case Some(_) => true
      case None => false
    }
  }

  def isNonEmptyGeom(wkb: String): Boolean = {
    val geom: Geometry = WKB.read(wkb)
    isNonEmptyGeom(geom)

  }

  def makeValidGeom(geom: Geometry): Geometry = {
    // try to make geometry valid. This is a basic trick, we might need to make this more sophisticated
    // There are some code samples here for JTS
    // https://stackoverflow.com/a/31474580/1410317
    val validGeom = {
      if (!geom.isValid) {
        val bufferedGeom = geom.buffer(0.0001).buffer(-0.0001)

        // the buffer can alter the geometry type and convert multi geometries to single geometries.
        // we want to preserve the original geometry type if possible.
        if (geom.getGeometryType == bufferedGeom.getGeometryType) bufferedGeom
        else if (geom.getGeometryType != bufferedGeom.getGeometryType && geom.getGeometryType
          .contains(bufferedGeom.getGeometryType))
          makeMultiGeom(bufferedGeom)
        else
          throw new RuntimeException(
            s"Faied to create a valid geometry: ${geom}"
          )
      } else geom
    }

    val normalizedGeom = reduce(gpr)(validGeom)
    normalizedGeom.normalize()
    normalizedGeom

  }

  def makeValidGeom(wkb: String): Geometry = {
    val geom: Geometry = WKB.read(wkb)
    makeValidGeom(geom)
  }

  def makeMultiGeom(geom: Geometry): Geometry = {
    geom match {
      case point: Point => MultiPoint(point)
      case line: LineString => MultiLineString(line)
      case polygon: Polygon => MultiPolygon(polygon)
      case _ =>
        throw new IllegalArgumentException(
          "Can only convert Point, LineString and Polygon to Multipart Geometries."
        )
    }
  }
}
