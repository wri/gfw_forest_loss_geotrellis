package org.globalforestwatch.util

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.rdd.RDD
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.util.ImplicitGeometryConverter.fromGeotrellisFeature
import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry}
import org.datasyslab.geospark.spatialRDD.SpatialRDD

object RDDAdapter {


  def toSpatialRDD(rdd: RDD[Feature[Geometry, FeatureId]]): SpatialRDD[GeoSparkGeometry] = {
    val spatialRDD = new SpatialRDD[GeoSparkGeometry]()
    spatialRDD.rawSpatialRDD = rdd.map(fromGeotrellisFeature[Geometry, FeatureId, GeoSparkGeometry])
      .toJavaRDD()
    spatialRDD.analyze()
    spatialRDD

  }
}
