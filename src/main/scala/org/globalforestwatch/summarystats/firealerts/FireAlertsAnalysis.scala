package org.globalforestwatch.summarystats.firealerts

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.{FireAlertModisFeatureId, FireAlertViirsFeatureId}
import org.globalforestwatch.features.{FeatureDF, FeatureFactory, FeatureId}
import org.globalforestwatch.util.Util._
import cats.data.NonEmptyList
import geotrellis.vector
import geotrellis.spark.{KeyBounds, SpatialKey}
import geotrellis.spark.partition.SpacePartitioner
import org.apache.spark.HashPartitioner

object FireAlertsAnalysis {
  def apply(featureRDD: RDD[Feature[vector.Geometry, FeatureId]],
            featureType: String,
            part: HashPartitioner,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
    val layoutDefinition = fireAlertType match {
      case "viirs" => ViirsGrid.blockTileGrid
      case "modis" => ModisGrid.blockTileGrid
    }

    val summaryRDD: RDD[(FeatureId, FireAlertsSummary)] =
      FireAlertsRDD(featureRDD, layoutDefinition, None, kwargs)

    val fireDF = fireAlertType match {
      case "viirs" => summaryRDD
        .flatMap {
          case (id, summary) =>
            summary.stats.map {
              case (dataGroup, data) => {
                id match {
                  case viirsId: FireAlertViirsFeatureId =>
                    FireAlertsRowViirs(viirsId, dataGroup, data)
                  case _ =>
                    throw new IllegalArgumentException("Not a valid Fire Alert ID")
                }
              }
            }
        }
        .toDF("fireId", "data_group", "data")
      case "modis" => summaryRDD
        .flatMap {
          case (id, summary) =>
            summary.stats.map {
              case (dataGroup, data) => {
                id match {
                  case modisId: FireAlertModisFeatureId =>
                    FireAlertsRowModis(modisId, dataGroup, data)
                  case _ =>
                    throw new IllegalArgumentException("Not a valid Fire Alert ID")
                }
              }
            }
        }
        .toDF("fireId", "data_group", "data")
    }

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

    val polyFeatureDF = FeatureDF(featureUris, featureObj, kwargs, spark)
    val featureViewName = featureObj.getClass.getSimpleName.dropRight(1).toLowerCase

    polyFeatureDF.createOrReplaceTempView(featureViewName)
    val polySpatialDf =
      spark.sql(
      s"""
         |SELECT ST_PrecisionReduce(ST_GeomFromWKB(geom), 13) AS polyshape, *
         |FROM $featureViewName
         |WHERE geom != '0106000020E610000000000000'
      """.stripMargin)

    polySpatialDf.createOrReplaceTempView(featureViewName)

    val polyStructIdDf = featureType match {
      case "gadm" =>
        val polyIdDf = polySpatialDf.select($"polyshape", $"gid_0" as "iso", $"gid_1" as "adm1", $"gid_2" as "adm2")
        polyIdDf.createOrReplaceTempView(featureViewName)
        spark.sql(
          s"""
             |SELECT polyshape, struct(iso, adm1, adm2) as featureId
             |FROM $featureViewName
          """.stripMargin)
      case "wdpa" =>
        val polyIdDf = polySpatialDf.select(
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
        val polyIdDf = polySpatialDf.select($"polyshape", $"fid" as "featureId")
        polyIdDf.createOrReplaceTempView(featureViewName)
        spark.sql(
          s"""
             |SELECT polyshape, struct(featureId) as featureId
             |FROM $featureViewName
          """.stripMargin)
      case "geostore" =>
        val polyIdDf = polySpatialDf.select($"polyshape", $"geostore_id" as "geostoreId")
        polyIdDf.createOrReplaceTempView(featureViewName)
        spark.sql(
          s"""
             |SELECT polyshape, struct(geostoreId) as featureId
             |FROM $featureViewName
          """.stripMargin)
    }


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
}
