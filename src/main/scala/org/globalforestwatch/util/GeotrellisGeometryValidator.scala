package org.globalforestwatch.util
import org.apache.log4j.Logger
import geotrellis.vector.{
  Geometry,
  GeomFactory,
  LineString,
  MultiPoint,
  Point,
  Polygon,
  MultiLineString,
  MultiPolygon
}
import geotrellis.vector.io.wkb.WKB
import org.globalforestwatch.util.GeotrellisGeometryReducer.{gpr, reduce}
import scala.util.Try

object GeotrellisGeometryValidator extends java.io.Serializable {
  val logger = Logger.getLogger("Geotrellis Geometry Validator")

  def isNonEmptyGeom(geom: Geometry): Boolean = {
    val maybeGeom: Option[Geometry] = try {
      Some(reduce(gpr)(geom))
    } catch {
      case _: java.lang.AssertionError =>
        println("There was an empty geometry")
        None
    }

    maybeGeom match {
      case Some(_) => true
      case None => false
    }
  }

  def isNonEmptyGeom(wkb: String): Boolean = {
    isNonEmptyGeom(makeValidGeom(wkb))
  }

  def makeValidGeom(geom: Geometry): Geometry = {
    val validGeom = {
      if (!geom.isValid) {

        val fixedGeom = GfwGeometryFixer.fix(geom)

        // Geometry fixer may alter the geometry type or even return an empty geometry
        // We want to try to preserve the geometry type if possible

        preserveGeometryType(fixedGeom, geom.getGeometryType)

      } else preserveGeometryType(geom, geom.getGeometryType)
    }

    validGeom.normalize()
    validGeom
  }

  def makeValidGeom(wkb: String): Geometry = {
    val geom: Option[Geometry] = Try(WKB.read(wkb)).toOption
    geom.map(makeValidGeom(_)).getOrElse(GeomFactory.factory.createPoint())
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

  private def preserveGeometryType(geom: Geometry,
                                   desiredGeometryType: String): Geometry = {
    if (desiredGeometryType != geom.getGeometryType && desiredGeometryType
      .contains(geom.getGeometryType)) {
      logger.warn(
        s"Fixed geometry of type ${geom.getGeometryType}. Cast to ${desiredGeometryType}."
      )
      makeMultiGeom(geom)
    } else if (desiredGeometryType != geom.getGeometryType) {
      logger.warn(
        s"Not able to preserve geometry type. Return ${geom.getGeometryType} instead of ${desiredGeometryType}"
      )
      geom
    } else geom
  }

}
