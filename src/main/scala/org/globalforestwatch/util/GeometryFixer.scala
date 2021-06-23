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
  MultiPolygon,
  Polygon,
  MultiLineString,
  LineString,
  Point,
  MultiPoint,
  GeometryCollection
}
import org.locationtech.jts.geom.{
  GeometryFactory,
  LinearRing,
  Coordinate,
  CoordinateArrays
}

case class GeometryFixer(geom: Geometry, keepCollapsed: Boolean = false) {

  val factory: GeometryFactory = geom.getFactory

  def fix(): Geometry = {
    if (geom.getNumGeometries == 0) {
      return geom.copy();
    }

    geom match {
      case geom: Point => fixPoint(geom)
      //  LinearRing must come before LineString
      case geom: LinearRing => fixLinearRing(geom)
      case geom: LineString => fixLineString(geom)
      case geom: Polygon => fixPolygon(geom)
      case geom: MultiPoint => fixMultiPoint(geom)
      case geom: MultiLineString => fixMultiLineString(geom)
      case geom: MultiPolygon => fixMultiPolygon(geom)
      case geom: GeometryCollection => fixCollection(geom)
      case _ => throw new UnsupportedOperationException(geom.getClass.getName)
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

    val holes: List[Geometry] = for {
      i <- geomRange

    } yield {

      val ring: LinearRing = toLinearRing(geom.getInteriorRingN(i))
      fixRing(ring)
    }

    val fix = holes.foldLeft(factory.createGeometry(factory.createPolygon())) {
      (acc: Geometry, hole: Geometry) =>
        acc.union(hole)
    }

    fix match {
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
      case Some(geom) => shell.difference(geom)
      case _ => shell
    }
  }

  private def fixPolygon(geom: Polygon): Geometry = {
    val fix = fixPolygonElement(geom)
    fix match {
      case Some(geom) => geom
      case _ => factory.createPolygon()
    }
  }

  private def fixPolygonElement(geom: Polygon): Option[Geometry] = {
    val shell: LinearRing = toLinearRing(geom.getExteriorRing)
    val fixShell: Geometry = fixRing(shell)

    if (fixShell.isEmpty && keepCollapsed)
      Some(fixLineString(shell))
    else if (fixShell.isEmpty && !keepCollapsed)
      None
    else if (geom.getNumInteriorRing == 0)
      Some(fixShell)
    else {
      val fixedHoles: Option[Geometry] = fixHoles(geom)
      Some(removeHoles(fixShell, fixedHoles))
    }
  }

  private def fixMultiPolygon(geom: MultiPolygon): Geometry = {

    val geomRange: List[Int] = List.range(0, geom.getNumGeometries)

    val fixed_parts: List[Option[Geometry]] = for {
      i <- geomRange

    } yield fixPolygonElement(geom.getGeometryN(i).asInstanceOf[Polygon])

    fixed_parts.foldLeft(factory.createGeometry(factory.createMultiPolygon())) {
      (acc, part) =>
        part match {
          case Some(geom) if !geom.isEmpty => acc.union(geom)
          case _ => acc
        }
    }

  }

  private def fixCollection(geom: GeometryCollection): Geometry = {

    val geomRange: List[Int] = List.range(0, geom.getNumGeometries)

    val fixed_parts: Array[Geometry] = (for {
      i <- geomRange
    } yield fix(geom.getGeometryN(i))).toArray

    factory.createGeometryCollection(fixed_parts)
  }
}
