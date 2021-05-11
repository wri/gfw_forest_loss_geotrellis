package org.globalforestwatch.features

import cats.data.NonEmptyList
import geotrellis.vector
import org.datasyslab.geospark.enums.GridType
import com.vividsolutions.jts.geom.Geometry
import org.datasyslab.geospark.spatialRDD.SpatialRDD
//import org.apache.sedona.core.enums.GridType
//import org.apache.sedona.sql.utils.Adapter
//import org.locationtech.jts.geom.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.util.Util._


object FeatureRDDFactory {
  def apply(analysis: String,
            featureType: String,
            featureUris: NonEmptyList[String],
            kwargs: Map[String, Any],
            spark: SparkSession): RDD[vector.Feature[vector.Geometry, FeatureId]] = {
    val featureObj = FeatureFactory(featureType).featureObj

    analysis match {
      case "firealerts" =>
        val fireRDD: SpatialRDD[Geometry] = FireAlertRDD(spark, kwargs)
        val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
        val fireAlertObj =
          FeatureFactory("firealerts", Some(fireAlertType)).featureObj

        fireRDD.spatialPartitioning(GridType.QUADTREE)

        FeatureRDD(fireAlertObj, fireRDD, kwargs)

      case "burned_areas" =>
        val burnedAreasUris: NonEmptyList[String] = getAnyMapValue[NonEmptyList[String]](
            kwargs,
            "burnedAreasSource"
        )

        val burnedAreasFeatureObj = FeatureFactory("burned_areas").featureObj

        val spatialRDD: SpatialRDD[Geometry] = PolygonIntersectionRDD(
          featureUris,
          featureObj,
          featureType,
          burnedAreasUris,
          burnedAreasFeatureObj,
          "burned_areas",
          spark,
          kwargs
        )
        spatialRDD.spatialPartitioning(GridType.QUADTREE)

        FeatureRDD(featureObj, spatialRDD, kwargs)
      case _ =>
        FeatureRDD(featureUris, featureObj, kwargs, spark)
    }
  }
}
