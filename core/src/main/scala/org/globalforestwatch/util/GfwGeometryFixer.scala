/*
 * This is a partial scala port of the JTS GeometryFixer.
 * This class is was not available in the JTS version currently used in Geotrellis.
 * Now it may perform additional fixes specific to this project.
 *
 *
 * https://github.com/locationtech/jts/blob/master/modules/core/src/main/java/org/locationtech/jts/geom/util/GeometryFixer.java
 * Copyright (c) 2021 Martin Davis.
 */

package org.globalforestwatch.util

import org.apache.log4j.Logger
import org.globalforestwatch.util.GeotrellisGeometryReducer.{gpr, reduce}
import org.locationtech.jts.geom.{Geometry, GeometryFactory, MultiPolygon, Polygon, TopologyException}
import org.locationtech.jts.geom.util.GeometryFixer
import org.locationtech.jts.operation.overlay.snap.GeometrySnapper
import org.locationtech.jts.operation.overlayng.OverlayNGRobust

import scala.collection.JavaConverters._
import scala.annotation.tailrec

case class GfwGeometryFixer(geom: Geometry, keepCollapsed: Boolean = false) {
  private val logger: Logger = Logger.getLogger("GfwGeometryFixer")
  private val factory: GeometryFactory = geom.getFactory
  private val snapTolerance: Double = 1 / math.pow(
    10,
    geom.getPrecisionModel.getMaximumSignificantDigits - 1
  )

  private val maxSnapTolerance: Double = snapTolerance * 1000
  def fix(): Geometry = {
    try {
      if (geom.getNumGeometries == 0) {
        geom.copy()
      } else {

        // doing a cheap trick here to eliminate sliver holes and other artifacts. However this might change geometry type.
        // so we need to do this early on to avoid winding code. This block is not part of the original Java implementation.
        val preFixedGeometry =
        geom match {
          case poly: Polygon => ironPolygons(poly)
          case multi: MultiPolygon => ironPolygons(multi)
          case _ => geom
        }

        GeometryFixer.fix(preFixedGeometry)
      }
    } catch {
      case e: Throwable => // TODO: narrow down exception
        println(s"Error fixing geometry: ${geom.toText}")
        throw e
    }
  }

  /** Ironing out potential sliver artifacts such as holes that resemble lines. Should only be used with Polygons or MultiPolygons.
    */
  private def ironPolygons(geom: Geometry): Geometry = {
    val bufferedGeom: Geometry = geom.buffer(0.0001).buffer(-0.0001).buffer(0)
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
          case geom: Polygon           => List(Some(geom))
//            if (!geom.isValid) {
//              val fixedGeom = geom.buffer(0)
//
//              if (!fixedGeom.isValid) {
//                throw new Exception(f"Geometry could not be fixed: ${geom}")
//              }
//
//              loop(fixedGeom)
//            } else {
//              List(Some(geom))
//            }
          case multiGeom: MultiPolygon => loop(multiGeom)
          case _                       => List()
        }
      }
      nested_polygons.flatten
    }

    val polygons: Array[Geometry] = loop(multiGeometry).toArray.flatten
    OverlayNGRobust.union(polygons.toList.asJava)
  }

  /** Poor man's implementation of JTS Overlay NG Robust difference (not part of current JTS version)
    */
  @tailrec
  private def difference(
    geom1: Geometry,
    geom2: Geometry,
    adjustedSnapTolerance: Double = snapTolerance
  ): Geometry = {
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

  /** Poor man's implementation of JTS Overlay NG Robust union (not part of current JTS version)
    */
  @tailrec
  private def union[T <: Geometry](
    parts: Array[Option[T]],
    baseGeometry: Geometry = factory.createPolygon(),
    adjustedSnapTolerance: Double = snapTolerance
  ): Geometry = {
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

object GfwGeometryFixer {
  def fix(geom: Geometry): Geometry = {
    GfwGeometryFixer(geom).fix()
  }
}