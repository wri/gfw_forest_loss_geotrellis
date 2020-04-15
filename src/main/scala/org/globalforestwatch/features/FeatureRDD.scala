package org.globalforestwatch.features

import cats.data.NonEmptyList
import geotrellis.vector.Geometry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object FeatureRDD {
  def apply(
     input: NonEmptyList[String],
     featureObj: Feature,
     kwargs: Map[String, Any],
     spark: SparkSession
  ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {
    val featuresDF: DataFrame =
      FeatureDF(input, featureObj, kwargs, spark)

    featuresDF.rdd.mapPartitions({ iter: Iterator[Row] =>
      for {
        i <- iter
        if featureObj.isValidGeom(i)
      } yield {
        featureObj.get(i)
      }
    }, preservesPartitioning = true)
  }
}
