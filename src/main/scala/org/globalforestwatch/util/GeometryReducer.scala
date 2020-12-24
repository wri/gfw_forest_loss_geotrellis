package org.globalforestwatch.util

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.locationtech.jts.precision.GeometryPrecisionReducer

object GeometryReducer extends java.io.Serializable {

  // We need to reduce geometry precision  a bit to avoid issues like reported here
  // https://github.com/locationtech/geotrellis/issues/2951
  //
  // Precision is set in src/main/resources/application.conf
  // Here we use a fixed precision type and scale 1e8
  // This is more than enough given that we work with 30 meter pixels
  // and geometries already simplified to 1e11

  val gpr = new GeometryPrecisionReducer(
    geotrellis.vector.GeomFactory.precisionModel
  )

  def reduce(
    gpr: org.locationtech.jts.precision.GeometryPrecisionReducer
  )(g: geotrellis.vector.Geometry): geotrellis.vector.Geometry =
    geotrellis.vector.GeomFactory.factory.createGeometry(gpr.reduce(g))

  def isValidGeom(wkb: String): Boolean = {
    val geom: Option[Geometry] = try {
      Some(reduce(gpr)(WKB.read(wkb)))
    } catch {
      case ae: java.lang.AssertionError =>
        println("There was an empty geometry")
        None
      case t: Throwable => throw t
    }

    geom match {
      case Some(g) => true
      case None    => false
    }
  }
}
