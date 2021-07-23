/*
 * This is a scala port of the JTS GeometryFixer.
 * This class is not available in the JTS version currently used in Geotrellis.
 *
 *
 * https://github.com/locationtech/jts/blob/master/modules/core/src/main/java/org/locationtech/jts/geom/util/GeometryFixer.java
 * Copyright (c) 2021 Martin Davis.
 */

package org.globalforestwatch.util

import geotrellis.vector.{
  Geometry,
  GeometryCollection,
  LineString,
  MultiLineString,
  MultiPoint,
  MultiPolygon,
  Point,
  Polygon
}
import org.apache.log4j.Logger
import org.globalforestwatch.util.GeotrellisGeometryReducer.{gpr, reduce}
import org.locationtech.jts.geom.{
  Coordinate,
  CoordinateArrays,
  GeometryFactory,
  LinearRing,
  TopologyException
}
import org.locationtech.jts.operation.overlay.snap.GeometrySnapper

import scala.annotation.tailrec

case class GeometryFixer(geom: Geometry, keepCollapsed: Boolean = false) {

  private val logger: Logger = Logger.getLogger("GeometryFixer")

  private val factory: GeometryFactory = geom.getFactory
  private val snapTolerance: Double = 1 / math.pow(
    10,
    geom.getPrecisionModel.getMaximumSignificantDigits - 1
  )
  private val maxSnapTolerance: Double = snapTolerance * 1000

  def fix(): Geometry = {

    if (geom.getNumGeometries == 0) {
      geom.copy()
    } else {

      // doing a cheap trick here to eliminate sliver holes and other artifacts. However this might change geometry type.
      // so we need to do this early on to avoid winding code. This block is not part of the original Java implementation.
      val preFixedGeometry =
      geom match {
        case poly: Polygon => ironPolgons(poly)
        case multi: MultiPolygon => ironPolgons(multi)
        case _ => geom
      }

      preFixedGeometry match {
        case geom: Point => fixPoint(geom)
        //  LinearRing must come before LineString
        case geom: LinearRing => fixLinearRing(geom)
        case geom: LineString => fixLineString(geom)
        case geom: Polygon => fixPolygon(geom)
        case geom: MultiPoint => fixMultiPoint(geom)
        case geom: MultiLineString => fixMultiLineString(geom)
        case geom: MultiPolygon => fixMultiPolygon(geom)
        case geom: GeometryCollection => fixCollection(geom)
        case _ =>
          throw new UnsupportedOperationException(
            preFixedGeometry.getClass.getName
          )
      }

    }
  }

  private def fix(geom: Geometry): Geometry = {
    GeometryFixer(geom).fix()
  }

  private def fixPoint(geom: Point): Geometry = {
    val point = fixPointElement(geom)
    point match {
      case Some(geom) => geom
      case _ => factory.createPoint
    }
  }

  private def fixPointElement(geom: Point): Option[Geometry] = {

    if (geom.isEmpty || !geom.isValid) None
    else Some(geom.copy)
  }

  private def fixMultiPoint(geom: MultiPoint): Geometry = {

    val geomRange: List[Int] = List.range(0, geom.getNumGeometries)

    val points: Array[Point] = (for {
      i <- geomRange
      if !geom.getGeometryN(i).isEmpty
    } yield fixPointElement(geom.getGeometryN(i).asInstanceOf[Point])).flatMap {
      case Some(geom: Point) => Seq(geom)
      case _ => Seq()
    }.toArray

    factory.createMultiPoint(points)

  }

  private def fixLinearRing(geom: LinearRing): Geometry = {
    val fix = fixLinearRingElement(geom)

    fix match {
      case Some(geom) => geom
      case _ => factory.createLinearRing
    }

  }

  private def fixLinearRingElement(geom: LinearRing): Option[Geometry] = {

    if (geom.isEmpty) None
    else {

      val pts = geom.getCoordinates
      val ptsFix = fixCoordinates(pts)

      if (keepCollapsed && ptsFix.length == 1)
        Some(factory.createPoint(ptsFix(0)))
      else if (keepCollapsed & ptsFix.length > 1 && ptsFix.length <= 3)
        Some(factory.createLineString(ptsFix))
      else if (ptsFix.length <= 3) None
      else {

        val ring = factory.createLinearRing(ptsFix)
        //--- convert invalid ring to LineString
        if (!ring.isValid) Some(factory.createLineString(ptsFix))
        else
          Some(ring)
      }
    }
  }

  private def fixCoordinates(pts: Array[Coordinate]) = {
    val ptsClean = CoordinateArrays.removeRepeatedPoints(pts).filter { coord =>
      !coord.x.isInfinite && !coord.y.isInfinite
    }
    CoordinateArrays.copyDeep(ptsClean);
  }

  private def fixLineString(geom: LineString): Geometry = {
    val fix: Option[Geometry] = fixLineStringElement(geom);

    fix match {
      case Some(geom) => geom
      case _ => factory.createLineString()
    }

  }

  private def fixLineStringElement(geom: LineString): Option[Geometry] = {
    geom match {
      case g if g.isEmpty => None
      case _ =>
        val pts: Array[Coordinate] = geom.getCoordinates
        val ptsFix: Array[Coordinate] = fixCoordinates(pts);
        if (keepCollapsed && ptsFix.length == 1) {
          Some(factory.createPoint(ptsFix(0)))
        } else if (ptsFix.length <= 1) {
          None
        } else {
          Some(factory.createLineString(ptsFix))
        }
    }

  }

  private def fixMultiLineString(geom: MultiLineString): Geometry = {
    val geomRange: List[Int] = List.range(0, geom.getNumGeometries)

    val lines: Array[LineString] = (for {
      i <- geomRange
      if !geom.getGeometryN(i).isEmpty
    } yield
      fixLineStringElement(geom.getGeometryN(i).asInstanceOf[LineString])).flatMap {

      case Some(geom: LineString) => Seq(geom)
      case _ => Seq() // Dropping potential points and empty geoms as well
    }.toArray

    //TODO: Return Geometry Collection in case there were points
    // https://github.com/locationtech/jts/blob/master/modules/core/src/main/java/org/locationtech/jts/geom/util/GeometryFixer.java#L246-L248
    factory.createMultiLineString(lines)

  }

  private def toLinearRing(line: LineString): LinearRing = {

    if (line.isClosed) factory.createLinearRing(line.getCoordinates)
    else // close ring in case it isn't
      factory.createLinearRing(
        line.getCoordinates ++ Array(line.getCoordinateN(0))
      )
  }

  private def fixHoles(geom: Polygon): Option[Geometry] = {

    val geomRange: List[Int] = List.range(0, geom.getNumInteriorRing)

    val holes: Array[Option[Geometry]] = (for {
      i <- geomRange

    } yield {

      val ring: LinearRing = toLinearRing(geom.getInteriorRingN(i))
      Some(fixRing(ring))
    }).toArray

    val unionGeom = union(holes, factory.createPolygon())

    unionGeom match {
      case geom if geom.isEmpty => None
      case _ => Some(geom)
    }
  }

  private def fixRing(ring: LinearRing): Geometry = {
    //-- always execute fix, since it may remove repeated coords etc
    val poly: Polygon = factory.createPolygon(ring);
    poly.buffer(0)

  }

  private def removeHoles(shell: Geometry,
                          holes: Option[Geometry]): Geometry = {
    holes match {
      case Some(geom) => difference(shell, geom)
      case _ => shell
    }
  }

  private def fixShell(geom: Polygon): Geometry = {
    val shell: LinearRing = toLinearRing(geom.getExteriorRing)
    fixRing(shell)
  }

  private def fixPolygon(geom: Polygon): Geometry = {

    val fix = fixPolygonElement(geom)
    fix match {
      case Some(geom) => geom
      case _ => factory.createPolygon()
    }
  }

  private def fixPolygonElement(geom: Polygon): Option[Geometry] = {

    val fixedShell: Geometry = fixShell(geom)

    if (fixedShell.isEmpty && keepCollapsed) {
      val shell: LinearRing = toLinearRing(geom.getExteriorRing)
      Some(fixLineString(shell))
    } else if (fixedShell.isEmpty && !keepCollapsed)
      None
    else if (geom.getNumInteriorRing == 0)
      Some(fixedShell)
    else {

      val fixedHoles: Option[Geometry] = fixHoles(geom)

      Some(removeHoles(fixedShell, fixedHoles))

    }
  }

  private def fixMultiPolygon(geom: MultiPolygon): Geometry = {

    val geomRange: List[Int] = List.range(0, geom.getNumGeometries)

    val fixedParts: Array[Option[Geometry]] = (for {
      i <- geomRange

    } yield
      fixPolygonElement(geom.getGeometryN(i).asInstanceOf[Polygon])).toArray

    union(fixedParts, factory.createMultiPolygon())

  }

  private def fixCollection(geom: GeometryCollection): Geometry = {

    val geomRange: List[Int] = List.range(0, geom.getNumGeometries)

    val fixed_parts: Array[Geometry] = (for {
      i <- geomRange
    } yield fix(geom.getGeometryN(i))).toArray

    factory.createGeometryCollection(fixed_parts)
  }

  private def ironPolgons(geom: Geometry): Geometry = {

    /**
      * Ironing out potential sliver artifacts such as holes that resemble lines.
      * Should only be used with Polygons or MultiPolygons.
      * */
    val bufferedGeom: Geometry = geom.buffer(0.0001).buffer(-0.0001)
    val polygons: Geometry = extractPolygons(bufferedGeom)
    reduce(gpr)(polygons)
  }

  private def extractPolygons(multiGeometry: Geometry): Geometry = {
    def loop(multiGeometry: Geometry): List[Option[Polygon]] = {

      val geomRange: List[Int] =
        List.range(0, multiGeometry.getNumGeometries)

      val nested_polygons = for {
        i <- geomRange

      } yield {
        multiGeometry.getGeometryN(i) match {
          case geom: Polygon => List(Some(geom))
          case multiGeom: MultiPolygon => loop(multiGeom)
          case _ => List()
        }
      }
      nested_polygons.flatten
    }

    val polygons: Array[Option[Polygon]] = loop(multiGeometry).toArray
    union(polygons)
  }

  @tailrec
  private def difference(
                          geom1: Geometry,
                          geom2: Geometry,
                          adjustedSnapTolerance: Double = snapTolerance
                        ): Geometry = {

    /**
      * Poor man's implementation of JTS Overlay NG Robust difference (not part of current JTS version)
      * */
    try {
      val snappedGeometries: Array[Geometry] =
        GeometrySnapper.snap(geom1, geom2, adjustedSnapTolerance)
      snappedGeometries match {
        case Array(snappedGeom1: Geometry, snappedGeom2: Geometry) =>
          snappedGeom1.difference(snappedGeom2)
      }
    } catch {
      case e: TopologyException =>
        if (adjustedSnapTolerance >= maxSnapTolerance) throw e
        else difference(geom1, geom2, adjustedSnapTolerance * 10)
    }
  }


  @tailrec
  private def union[T <: Geometry](parts: Array[Option[T]],
                                   baseGeometry: Geometry = factory.createPolygon(),
                                   adjustedSnapTolerance: Double = snapTolerance): Geometry = {

    /**
      * Poor man's implementation of JTS Overlay NG Robust union (not part of current JTS version)
      * */
    try {
      parts.foldLeft(factory.createGeometry(baseGeometry)) { (acc, part) =>
        part match {

          // Snap the first geometry to itself
          case Some(geom) if acc.isEmpty && !geom.isEmpty =>
            GeometrySnapper.snapToSelf(geom, adjustedSnapTolerance, true)

          // Afterwards snap geometries to each other
          case Some(geom) if !geom.isEmpty =>
            val snappedGeometries: Array[Geometry] =
              GeometrySnapper.snap(geom, acc, adjustedSnapTolerance)
            snappedGeometries match {
              case Array(snappedGeom: Geometry, snappedAcc: Geometry) =>
                snappedAcc.union(snappedGeom)
            }

          // Or simply return accumulator in case part is emtpy
          case _ => acc
        }
      }
    } catch {
      case e: TopologyException =>
        // In case there is a topology error increase snap Tolerance by factor 10
        if (adjustedSnapTolerance > maxSnapTolerance) throw e
        else {
          logger.debug(
            s"Adjust snap tolerance to ${adjustedSnapTolerance * 10}"
          )
          union(parts, baseGeometry, adjustedSnapTolerance * 10)
        }

    }

  }

}
