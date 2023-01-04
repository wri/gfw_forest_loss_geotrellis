package org.globalforestwatch.util

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.rdd.RDD
import org.globalforestwatch.features.FeatureId
import org.locationtech.jts.geom.Geometry
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.summarystats.Location
import org.globalforestwatch.util.GeotrellisGeometryValidator.{makeValidGeom, preserveGeometryType}
import org.locationtech.jts.geom.util.GeometryFixer

object RDDAdapter {


  def toSpatialRDD(rdd: RDD[Feature[Geometry, FeatureId]]): SpatialRDD[Geometry] = {
    val spatialRDD = new SpatialRDD[Geometry]()
    spatialRDD.rawSpatialRDD = rdd.map { feature =>
      feature.geom.setUserData(feature.data)
      feature.geom
    }.toJavaRDD()
    spatialRDD.analyze()
    spatialRDD
  }

  def toSpatialRDDfromLocationRdd(rdd: RDD[Location[Geometry]], spark: SparkSession): SpatialRDD[Geometry] = {
    val spatialRDD = new SpatialRDD[Geometry]()
    spatialRDD.rawSpatialRDD = rdd.map { case Location(id, geom) =>
      geom.setUserData(id)
      geom
    }.toJavaRDD()

    spatialRDD.analyze()
    spatialRDD
  }
}
