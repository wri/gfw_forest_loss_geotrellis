package org.globalforestwatch.features

import geotrellis.vector.Geometry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object FeatureRDD {
  def apply(
    featuresDF: DataFrame,
    featureObj: Feature
  ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {
    featuresDF.rdd.mapPartitions({ iter: Iterator[Row] =>
      for {
        i <- iter
        if featureObj.isValidGeom(i)
      } yield {
        featureObj.getFeature(i)
      }
    }, preservesPartitioning = true)
  }

}
