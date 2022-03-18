package org.globalforestwatch.summarystats.firealerts

import org.locationtech.jts.geom.Geometry
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.sql.utils.Adapter
import geotrellis.vector.Feature
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.globalforestwatch.features._
import org.globalforestwatch.util.Util._
import cats.data.NonEmptyList
import geotrellis.vector
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.globalforestwatch.summarystats.{SummaryAnalysis}
import org.apache.spark.sql.catalyst.ScalaReflection

import scala.util.{Failure, Success, Try}


object FireAlertsAnalysis extends SummaryAnalysis {

  val name = "firealerts"

  def apply(featureRDD: RDD[Feature[vector.Geometry, FeatureId]],
            featureType: String,
            featureFilter: FeatureFilter,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
    val layoutDefinition = fireAlertType match {
      case "viirs" | "burned_areas" => ViirsGrid.blockTileGrid
      case "modis" => ModisGrid.blockTileGrid
    }

    val partition = fireAlertType match {
      case "modis" | "viirs" | "burned_areas" => false
      case _ => true
    }

    val summaryRDD: RDD[(FeatureId, FireAlertsSummary)] =
      FireAlertsRDD(featureRDD, layoutDefinition, kwargs, partition = partition)

    val summaryDF = fireAlertType match {
      case "modis" | "viirs" =>
        joinWithFeatures(summaryRDD, featureType, featureFilter, spark, kwargs)
      case "burned_areas" =>
        FireAlertsDFFactory(featureType, summaryRDD, spark, kwargs).getDataFrame
    }

    // summaryDF.repartition(partitionExprs = $"featureId")

    val runOutputUrl: String = getOutputUrl(kwargs, s"${name}_${fireAlertType}")

    FireAlertsExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }

  def joinWithFeatures(summaryRDD: RDD[(FeatureId, FireAlertsSummary)],
                       featureType: String,
                       featureFilter: FeatureFilter,
                       spark: SparkSession,
                       kwargs: Map[String, Any]): DataFrame = {
    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
    val fireDF = FireAlertsDFFactory(featureType, summaryRDD, spark, kwargs).getDataFrame
    val firePointDF = fireDF
      .selectExpr("ST_Point(CAST(fireId.lon AS Decimal(24,10)),CAST(fireId.lat AS Decimal(24,10))) AS pointshape", "*")

    val featureUris: NonEmptyList[String] = getAnyMapValue[NonEmptyList[String]](kwargs, "featureUris")

    val featureDF = SpatialFeatureDF(featureUris, featureType, featureFilter, "geom", spark)

    // Using RDD instead of DF spatial join for performance options
//    firePointDF
//      .join(featureDF)
//      .where("ST_Intersects(pointshape, polyshape)")

    val fireRDD = Adapter.toSpatialRdd(firePointDF, "pointshape")
    val featureRDD = Adapter.toSpatialRdd(featureDF, "polyshape")

    fireRDD.analyze()
    featureRDD.analyze()

    // # of partitions must be less than half the total number of records for the spatial partitioner,
    // so only increase partitions if there are enough records
    val partitionMultiplier = if (fireRDD.approximateTotalCount > 128) 64 else 1
    fireRDD.spatialPartitioning(GridType.KDBTREE, fireRDD.rawSpatialRDD.getNumPartitions * partitionMultiplier)
    featureRDD.spatialPartitioning(fireRDD.getPartitioner)

    val buildOnSpatialPartitionedRDD = true
    val usingIndex = true
    val considerBoundaryIntersection = true

    fireRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

    val resultPairRDD = JoinQuery.SpatialJoinQueryFlat(fireRDD, featureRDD, usingIndex, considerBoundaryIntersection)
    //  resultPairRDD.take(10).foreach(println)

    pairRddToDf(resultPairRDD, featureType, fireAlertType, spark)
  }

  def pairRddToDf(pairRdd: JavaPairRDD[Geometry, Geometry], featureType: String, fireAlertType: String, spark: SparkSession): DataFrame = {
    val featureIdType = getFeatureIdStruct(featureType)
    val fireIdType = getFeatureIdStruct(fireAlertType)
    val dataGroupType = ScalaReflection.schemaFor[FireAlertsDataGroup].dataType.asInstanceOf[StructType]
    val dataType = ScalaReflection.schemaFor[FireAlertsData].dataType.asInstanceOf[StructType]

    val rowRdd = pairRdd.rdd.map[Row](f => {
      val seq1 = f._1.getUserData.toString.split("\t").toSeq
      val seq2 = f._2.getUserData.toString.split("\t").toSeq

      val featureId = Row.fromSeq(convert(seq1(0).substring(1, seq1(0).length - 1).split(',').toList, featureIdType.toList))
      val fireId = Row.fromSeq(convert(seq2(0).substring(1, seq2(0).length - 1).split(',').toList, fireIdType.toList))
      val dataGroup = Row.fromSeq(convert(seq2(1).substring(1, seq2(1).length - 1).split(',').toList, dataGroupType.toList))
      val data = Row.fromSeq(convert(seq2(2).substring(1, seq2(2).length - 1).split(',').toList, dataType.toList))

      val result = Seq(f._1.toString, featureId, f._2.toString, fireId, dataGroup, data)
      Row.fromSeq(result)
    })

    val schema =
      StructType(List(
        StructField("polyshape", StringType),
        StructField("featureId", featureIdType),
        StructField("pointshape", StringType),
        StructField("fireId", fireIdType),
        StructField("data_group", dataGroupType),
        StructField("data", dataType),
      ))

    spark.createDataFrame(rowRdd, schema)
  }

  def getFeatureIdStruct(featureType: String): StructType = {
    val featureIdSchema = featureType match {
      case "gadm" => ScalaReflection.schemaFor[GadmFeatureId]
      case "geostore" => ScalaReflection.schemaFor[GeostoreFeatureId]
      case "wdpa" => ScalaReflection.schemaFor[WdpaFeatureId]
      case "feature" => ScalaReflection.schemaFor[SimpleFeatureId]
      case "viirs" => ScalaReflection.schemaFor[FireAlertViirsFeatureId]
      case "modis" => ScalaReflection.schemaFor[FireAlertModisFeatureId]
    }

    featureIdSchema.dataType.asInstanceOf[StructType]
  }

  def checkNullable(field: StructField, value: String, parseAttempt: Try[Any]): Any = {
    parseAttempt match {
      case Success(parsed) => parsed
      case Failure(e) =>
        if (field.nullable) {
          null
        } else {
          println(s"Unable to parse non-nullable field ${field.name} (type ${field.dataType}) with value ${value}")
          throw e
        }
    }
  }

  def convert(values: List[String], fields: List[StructField]): List[Any] = {
    fields.zip(values).map {
      case (field: StructField, value: String) =>
        field.dataType match {
          case StringType => value
          case DoubleType => checkNullable(field, value, Try(value.toDouble))
          case IntegerType => checkNullable(field, value, Try(value.toInt))
          case BooleanType => checkNullable(field, value, Try(value.toBoolean))
          case _ => throw new IllegalArgumentException(s"Unexpected data type ${field.dataType.toString} for field ${field.name} with value ${value}")
        }
    }
  }
}
