package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.NonEmptyList

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import geotrellis.vector.{Feature, Geometry}
import org.apache.sedona.core.enums.{FileDataSplitter, GridType, IndexType}
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialRDD.PointRDD
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import org.globalforestwatch.features.{FeatureDF, FeatureFactory, FeatureId, FeatureRDD}
import org.globalforestwatch.util.Util.getAnyMapValue

object ForestChangeDiagnosticAnalysis {
  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    fireStats(featureType, spark, kwargs)
    //
    //    val summaryRDD: RDD[(FeatureId, ForestChangeDiagnosticSummary)] =
    //      ForestChangeDiagnosticRDD(
    //        featureRDD,
    //        ForestChangeDiagnosticGrid.blockTileGrid,
    //        kwargs
    //      )
    //
    //    val summaryDF =
    //      ForestChangeDiagnosticDFFactory(featureType, summaryRDD, spark).getDataFrame
    //
    //    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
    //      "/forest_change_diagnostic_" + DateTimeFormatter
    //      .ofPattern("yyyyMMdd_HHmm")
    //      .format(LocalDateTime.now)
    //
    //    ForestChangeDiagnosticExport.export(
    //      featureType,
    //      summaryDF,
    //      runOutputUrl,
    //      kwargs
    //    )
  }

  def fireStats(featureType: String,
                spark: SparkSession,
                kwargs: Map[String, Any]): Unit = {

    SedonaSQLRegistrator.registerAll(spark)


    // FIRE RDD
    val fireAlertType: String = getAnyMapValue[String](kwargs, "fireAlertType")
    val fireAlertObj = FeatureFactory(fireAlertType).featureObj
    val fireAlertUris: NonEmptyList[String] = getAnyMapValue[NonEmptyList[String]](kwargs, "fireAlertSource")
    val fireAlertPointDF = FeatureDF(fireAlertUris, fireAlertObj, kwargs, spark, "longitude", "latitude")
    val fireAlertSpatialRDD = Adapter.toSpatialRdd(fireAlertPointDF, "pointshape")


    // Feature RDD

    val featureObj = FeatureFactory(featureType).featureObj
    val featureUris: NonEmptyList[String] = getAnyMapValue[NonEmptyList[String]](kwargs, "featureUris")
    val featurePolygonDF = FeatureDF(featureUris, featureObj, featureType, kwargs, spark, "geom")
    val featureSpatialRDD = Adapter.toSpatialRdd(featurePolygonDF, "polyshape")

    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    val considerBoundaryIntersection = false // Only return gemeotries fully covered by each query window in queryWindowRDD
    val usingIndex = false

    fireAlertSpatialRDD.spatialPartitioning(GridType.QUADTREE)

    featureSpatialRDD.spatialPartitioning(fireAlertSpatialRDD.getPartitioner)
    featureSpatialRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

    val pairRDD = JoinQuery.SpatialJoinQuery(fireAlertSpatialRDD, featureSpatialRDD, usingIndex, considerBoundaryIntersection)

    pairRDD.take(10).forEach(println)


    //      featureRDD.toDF("polyshape", "location_id")
    //      .join(fireAlertPointDF)
    //      .where("ST_Intersects(pointshape, polyshape)")
    //      .select(col("location_id"), substring(col("acq_date"), 0, 4).as("year"))
    //      .groupBy("location_id", "year")
    //      .agg(count("location_id"))
  }


}
