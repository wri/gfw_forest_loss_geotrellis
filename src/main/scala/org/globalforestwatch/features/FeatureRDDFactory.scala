package org.globalforestwatch.features

import cats.data.NonEmptyList
import geotrellis.vector
import org.datasyslab.geospark.enums.GridType
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
//import org.apache.sedona.core.enums.GridType
//import org.apache.sedona.sql.utils.Adapter
//import org.locationtech.jts.geom.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.util.Util._

object FeatureRDDFactory {
  def apply(
             analysis: String,
             featureType: String,
             featureUris: NonEmptyList[String],
             kwargs: Map[String, Any],
             spark: SparkSession
           ): RDD[vector.Feature[vector.Geometry, FeatureId]] = {

    analysis match {
      case "firealerts" =>
        val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")

        fireAlertType match {
          case "viirs" | "modis" =>
            val fireRDD: SpatialRDD[Geometry] = FireAlertRDD(spark, kwargs)
            fireRDD.spatialPartitioning(GridType.QUADTREE)

            FeatureRDD(fireAlertType, fireRDD, kwargs)
          case "burned_areas" =>
            val burnedAreasUris: NonEmptyList[String] =
              getAnyMapValue[NonEmptyList[String]](kwargs, "fireAlertSource")

            FeatureRDD(
              fireAlertType,
              burnedAreasUris,
              ",",
              featureType,
              featureUris,
              "\t",
              kwargs,
              spark
            )
        }
      case _ =>
        FeatureRDD(featureUris, featureType, kwargs, spark)
    }
  }
}
