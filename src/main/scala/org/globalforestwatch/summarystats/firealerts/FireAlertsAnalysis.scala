package org.globalforestwatch.summarystats.firealerts

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geotrellis.vector.Feature
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureDF, FeatureFactory, FeatureId}
import org.globalforestwatch.util.Util._
import cats.data.NonEmptyList
import geotrellis.vector

object FireAlertsAnalysis {
  def apply(featureRDD: RDD[Feature[vector.Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
    val layoutDefinition = fireAlertType match {
      case "viirs" => ViirsGrid.blockTileGrid
      case "modis" => ModisGrid.blockTileGrid
    }

    val summaryRDD: RDD[(FeatureId, FireAlertsSummary)] =
      FireAlertsRDD(featureRDD, layoutDefinition, kwargs, partition = false)

    val fireDF = FireAlertsDFFactory(summaryRDD, spark, kwargs).getDataFrame

    val fireViewName = "fire_alerts"
    fireDF.createOrReplaceTempView(fireViewName)
    val firePointDF = spark.sql(
      s"""
         |SELECT ST_Point(CAST(fireId.lon AS Decimal(24,10)),CAST(fireId.lat AS Decimal(24,10))) AS pointshape, *
         |FROM $fireViewName
      """.stripMargin)

    firePointDF.createOrReplaceTempView(fireViewName)

    val featureObj = FeatureFactory(featureType).featureObj
    val featureUris: NonEmptyList[String] = getAnyMapValue[NonEmptyList[String]](kwargs, "featureUris")

    val polySpatialDf = FeatureDF(featureUris, featureObj, kwargs, spark, "geom")
    val featureViewName = featureObj.getClass.getSimpleName.dropRight(1).toLowerCase

    val polyStructIdDf = getFeatureDataframe(featureType, polySpatialDf, featureViewName, spark)
    polyStructIdDf.createOrReplaceTempView(featureViewName)

    val joined = spark.sql(
      s"""
         |SELECT $featureViewName.*, $fireViewName.*
         |FROM $fireViewName, $featureViewName
         |WHERE ST_Intersects($fireViewName.pointshape, $featureViewName.polyshape)
      """.stripMargin
    )

    joined.repartition(partitionExprs = $"featureId")

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      s"/firealerts_${fireAlertType}_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    FireAlertsExport.export(
      featureType,
      joined,
      runOutputUrl,
      kwargs
    )
  }

  def getFeatureDataframe(featureType: String, featureDF: DataFrame, featureViewName: String, spark: SparkSession): DataFrame = {
    import spark.implicits._

    featureType match {
      case "gadm" =>
        val polyIdDf = featureDF.select($"polyshape", $"gid_0" as "iso", $"gid_1" as "adm1", $"gid_2" as "adm2")
        polyIdDf.createOrReplaceTempView(featureViewName)
        spark.sql(
          s"""
             |SELECT polyshape, struct(iso, adm1, adm2) as featureId
             |FROM $featureViewName
          """.stripMargin)
      case "wdpa" =>
        val polyIdDf = featureDF.select(
          $"polyshape", $"wdpaid" as "wdpaid",
          $"name" as "name",
          $"iucn_cat" as "iucnCat",  $"iso", $"status")

        polyIdDf.createOrReplaceTempView(featureViewName)
        spark.sql(
          s"""
             |SELECT polyshape, struct(wdpaId, name, iucnCat, iso, status) as featureId
             |FROM $featureViewName
          """.stripMargin)
      case "feature" =>
        val polyIdDf = featureDF.select($"polyshape", $"fid" as "featureId")
        polyIdDf.createOrReplaceTempView(featureViewName)
        spark.sql(
          s"""
             |SELECT polyshape, struct(featureId) as featureId
             |FROM $featureViewName
          """.stripMargin)
      case "geostore" =>
        val polyIdDf = featureDF.select($"polyshape", $"geostore_id" as "geostoreId")
        polyIdDf.createOrReplaceTempView(featureViewName)
        spark.sql(
          s"""
             |SELECT polyshape, struct(geostoreId) as featureId
             |FROM $featureViewName
          """.stripMargin)
    }
  }
}
