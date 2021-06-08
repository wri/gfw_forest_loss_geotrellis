package org.globalforestwatch.util

import geotrellis.vector.Geometry
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
      if (!geom.isValid) geom.buffer(0.0001).buffer(-0.0001)
      else geom
    }

    val normalizedGeom = reduce(gpr)(validGeom)
    normalizedGeom.normalize()
    normalizedGeom

  }

  def makeValidGeom(wkb: String): Geometry = {
    val geom: Geometry = WKB.read(wkb)
    makeValidGeom(geom)
  }
}
