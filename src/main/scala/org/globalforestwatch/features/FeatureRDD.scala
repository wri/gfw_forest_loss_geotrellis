package org.globalforestwatch.features

import cats.data.NonEmptyList
import org.locationtech.jts.geom._
import org.apache.log4j.Logger
import geotrellis.store.index.zcurve.Z2
import geotrellis.vector
import geotrellis.vector.{Geometry, MultiPolygon}
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.spark.HashPartitioner
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter
import org.globalforestwatch.util.{GridRDD, SpatialJoinRDD}
import org.globalforestwatch.util.IntersectGeometry.{extractPolygons, intersectGeometries}

object FeatureRDD {
  val logger = Logger.getLogger("FeatureRDD")

  def apply(input: NonEmptyList[String],
            featureType: String,
            filters: FeatureFilter,
            splitFeatures: Boolean,
            spark: SparkSession,
           ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    if (splitFeatures)
      splitGeometries(input, featureType, filters, spark)
    else {
      val featureObj: Feature = Feature(featureType)
      val featuresDF: DataFrame =
        FeatureDF(input, featureObj, filters, spark)

      featuresDF.rdd
        .mapPartitions({ iter: Iterator[Row] =>
          for {
            i <- iter
            if featureObj.isNonEmptyGeom(i)
          } yield {
            featureObj.get(i)
          }
        }, preservesPartitioning = true)
    }
  }

  /*
    Convert point-in-polygon join to feature RDD
   */
  def pointInPolygonJoinAsFeature(
    featureType: String,
    spatialRDD: SpatialRDD[Geometry]
  ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {
    val scalaRDD =
      org.apache.spark.api.java.JavaRDD.toRDD(spatialRDD.spatialPartitionedRDD)

    scalaRDD
      .map {
        case pt: Point =>
          val pointFeatureData = pt.getUserData.asInstanceOf[String] //.split('\t')

          // use implicit geometry converter
          val geom: vector.Point = pt

          val pointFeatureId: FeatureId =
            FeatureId.fromUserData(featureType, pointFeatureData)

          vector.Feature(geom, pointFeatureId)
        case _ =>
          throw new IllegalArgumentException(
            "Point-in-polygon intersection must be points."
          )
      }

  }

  /*
    Convert polygon-polygon intersection join to feature RDD
   */
  def apply(
             feature1Type: String,
             feature1Uris: NonEmptyList[String],
             feature1Delimiter: String,
             feature2Type: String,
             feature2Uris: NonEmptyList[String],
             feature2Delimiter: String,
             feature1Filters: FeatureFilter,
             feature2Filters: FeatureFilter,
             spark: SparkSession
           ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    val feature1DF: DataFrame =
      SpatialFeatureDF(feature1Uris, feature1Type, feature1Filters, "geom", spark, feature1Delimiter)

    val feature2DF: DataFrame =
      SpatialFeatureDF(feature2Uris, feature2Type, feature2Filters, "geom", spark, feature2Delimiter)

    val feature1RDD = Adapter.toSpatialRdd(feature1DF, "polyshape")
    val feature2RDD = Adapter.toSpatialRdd(feature2DF, "polyshape")

    feature1RDD.analyze()
    feature2RDD.analyze()

    feature1RDD.spatialPartitioning(GridType.KDBTREE)
    feature2RDD.spatialPartitioning(feature1RDD.getPartitioner)

    val buildOnSpatialPartitionedRDD = true
    val usingIndex = true
    val considerBoundaryIntersection = true

    feature1RDD.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD)

    val resultPairRDD = JoinQuery.SpatialJoinQueryFlat(feature1RDD, feature2RDD, usingIndex, considerBoundaryIntersection)

    val pairedRDD = resultPairRDD.rdd.map { row =>
      val featureId1: FeatureId = FeatureId.fromUserData(feature1Type, row._2.getUserData.toString, ",")
      val featureId2: FeatureId = FeatureId.fromUserData(feature2Type, row._1.getUserData.toString, ",")
      val geom = row._1.intersection(row._2)

      (CombinedFeatureId(featureId1, featureId2), geom)
    }

    pairedRDD.flatMap {
      case (id, geom) =>
        geom match {
          case geomCol: GeometryCollection =>
            val maybePoly = extractPolygons(geomCol)
            maybePoly match {
              case Some(poly) => List(vector.Feature(poly, id))
              case _ => List()
            }
          case poly: Polygonal =>
            List(vector.Feature(poly, id))
          case _ =>
            // Polygon-polygon intersections can generate points or lines, which we just want to ignore
            logger.warn("Cannot process geometry type")
            List()
        }
    }
  }

  private def splitGeometries(
                               input: NonEmptyList[String],
                               featureType: String,
                               filters: FeatureFilter,
                               spark: SparkSession
                             ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    val featureDF: DataFrame =
      SpatialFeatureDF(input, featureType, filters, "geom", spark)

    val spatialRDD: SpatialRDD[Geometry] =
      Adapter.toSpatialRdd(featureDF, "polyshape")
    spatialRDD.analyze()

    spatialRDD.rawSpatialRDD = spatialRDD.rawSpatialRDD.rdd.map { geom: Geometry =>
      val featureId = FeatureId.fromUserData(featureType, geom.getUserData.asInstanceOf[String], delimiter = ",")
      geom.setUserData(featureId)
      geom
    }.repartition(numPartitions = spatialRDD.rawSpatialRDD.getNumPartitions * 10)

    splitGeometries(spatialRDD, spark)
  }

  def splitGeometries(
                       spatialFeatureRDD: SpatialRDD[Geometry],
                       spark: SparkSession
                     ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    val envelope: Envelope = spatialFeatureRDD.boundaryEnvelope

    val spatialGridRDD = GridRDD(envelope, spark, clip = true)
    val flatJoin: JavaPairRDD[Polygon, Geometry] =
      SpatialJoinRDD.flatSpatialJoin(
        spatialFeatureRDD,
        spatialGridRDD,
        considerBoundaryIntersection = true
      )

    /*
      partitions will come back very skewed and we will need to even them out for any downstream analysis
      For the summary analysis we will eventually use a range partitioner.
      However, the range partitioner uses sampling to come up with the  break points for the different partitions.
      If the input RDD is already heavily skewed, sampling will be off and the range partitioner won't do a good job.
     */
    val hashPartitioner = new HashPartitioner(flatJoin.getNumPartitions)

    flatJoin.rdd
      .keyBy({ pair: (Polygon, Geometry) =>
        Z2(
          (pair._1.getCentroid.getX * 100).toInt,
          (pair._1.getCentroid.getY * 100).toInt
        ).z
      })
      .partitionBy(hashPartitioner)
      .flatMap {
        case (_, (gridCell, geom)) =>
          val geometries = intersectGeometries(geom, gridCell)
          geometries
      }
      .flatMap { intersection =>
        // use implicit converter to covert to Geotrellis Geometry
        val geotrellisGeom: MultiPolygon = intersection

        if (!geotrellisGeom.isEmpty) {

          val userData = intersection.getUserData
          val featureId = userData match {
            case fid: FeatureId => fid
          }

          Seq(
            geotrellis.vector.Feature(
              geotrellisGeom,
              featureId
            )
          )
        }
        else Seq()
      }
  }

}
