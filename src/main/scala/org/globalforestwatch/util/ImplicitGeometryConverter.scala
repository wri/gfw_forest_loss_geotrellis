package org.globalforestwatch.util

import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry}
import com.vividsolutions.jts.io.WKBReader
import geotrellis.vector.{Feature, Geometry}
import geotrellis.vector.io.wkb.WKB
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.util.GeoSparkGeometryConstructor.toWKB
import org.globalforestwatch.util.GeotrellisGeometryValidator.makeValidGeom

import scala.reflect.ClassTag

object ImplicitGeometryConverter {
  implicit def toGeotrellisGeometry[I <: GeoSparkGeometry, O <: Geometry](
                                                                           geom: I
                                                                         ): O = {
    val wkb = toWKB(geom)
    val newGeom = makeValidGeom(wkb).asInstanceOf[O]
    newGeom.setUserData(geom.getUserData)
    newGeom.setSRID(geom.getSRID)
    newGeom
  }

  implicit def toGeoSparkGeometry[I <: Geometry, O <: GeoSparkGeometry](
                                                                         geom: I
                                                                       ): O = {
    val wkb: Array[Byte] = WKB.write(geom)
    val reader = new WKBReader()
    val newGeom = reader.read(wkb).asInstanceOf[O]
    newGeom.setUserData(geom.getUserData)
    newGeom.setSRID(geom.getSRID)
    newGeom
  }

  implicit def fromGeotrellisFeature[G <: Geometry,
    F <: FeatureId,
    O <: GeoSparkGeometry](feature: Feature[G, F]): O = {
    val newGeom: O = feature.geom
    newGeom.setUserData(feature.data)
    newGeom
  }

  implicit def toGeotrellisFeature[G <: Geometry,
    F <: FeatureId,
    I <: GeoSparkGeometry,
  ](geom: I): Feature[G, F] = {

    val newGeom: G = geom
    val featureId: F = geom.getUserData.asInstanceOf[F]
    Feature[G, F](newGeom, featureId)

  }

}
