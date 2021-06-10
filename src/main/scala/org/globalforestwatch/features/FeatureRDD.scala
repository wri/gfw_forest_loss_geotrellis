package org.globalforestwatch.features

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.{
  Envelope => GeoSparkEnvelope,
  Geometry => GeoSparkGeometry,
  Point => GeoSparkPoint,
  Polygon => GeoSparkPolygon,
  Polygonal => GeoSparkPolygonal,
  GeometryCollection => GeoSparkGeometryCollection
}
import org.apache.log4j.Logger
import geotrellis.store.index.zcurve.Z2
import geotrellis.vector
import geotrellis.vector.{Geometry, MultiPolygon}
import org.apache.spark.HashPartitioner
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.util.{GridRDD, SpatialJoinRDD}
import org.globalforestwatch.util.IntersectGeometry.{
  extractPolygons,
  intersectGeometries
}
import org.globalforestwatch.util.Util.getAnyMapValue
import org.globalforestwatch.util.ImplicitGeometryConverter._

object FeatureRDD {
  val logger = Logger.getLogger("FeatureRDD")

  def apply(input: NonEmptyList[String],
            featureType: String,
            kwargs: Map[String, Any],
            spark: SparkSession,
           ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    val splitFeatures = getAnyMapValue[Boolean](kwargs, "splitFeatures")
    //    val geomFieldName = getAnyMapValue[String](kwargs, "geomFieldName")

    if (splitFeatures) splitGeometries(input, featureType, kwargs, spark)
    else {
      val featureObj: Feature = FeatureFactory(featureType).featureObj
      val featuresDF: DataFrame =
        FeatureDF(input, featureObj, kwargs, spark)

      featuresDF.rdd
        .mapPartitions({ iter: Iterator[Row] =>
          for {
            i <- iter
            if featureObj.isValidGeom(i)
          } yield {
            featureObj.get(i)
          }
        }, preservesPartitioning = true)
    }
  }

  /*
    Convert point-in-polygon join to feature RDD
   */
  def apply(featureType: String,
            spatialRDD: SpatialRDD[GeoSparkGeometry],
            kwargs: Map[String, Any],
           ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    val scalaRDD =
      org.apache.spark.api.java.JavaRDD.toRDD(spatialRDD.spatialPartitionedRDD)

    scalaRDD
      .map {
        case pt: GeoSparkPoint =>
          val pointFeatureData = pt.getUserData.asInstanceOf[String] //.split('\t')

          // use implicit geometry converter
          val geom: vector.Point = pt

          val pointFeatureId: FeatureId =
            FeatureIdFactory(featureType).fromUserData(pointFeatureData)

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
             kwargs: Map[String, Any],
             spark: SparkSession
           ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    val spatialDF: DataFrame = PolygonIntersectionDF(
      feature1Uris,
      feature1Type,
      feature2Uris,
      feature2Type,
      spark,
      kwargs,
      feature2Delimiter = ","
    )

    val pairedRDD = spatialDF.rdd.map { row: Row =>
      val featureId1: FeatureId = FeatureIdFactory(feature1Type)
        .fromUserData(row.getAs[Row](0).toString, ",")
      val featureId2: FeatureId = FeatureIdFactory(feature2Type)
        .fromUserData(row.getAs[Row](1).toString, ",")
      val geom = row.getAs[GeoSparkGeometry](2)

      (CombinedFeatureId(featureId1, featureId2), geom)
    }

    val hashPartitioner = new HashPartitioner(pairedRDD.getNumPartitions)
    pairedRDD
      .keyBy({ pair: (CombinedFeatureId, GeoSparkGeometry) =>
        Z2(
          (pair._2.getCentroid.getX * 100).toInt,
          (pair._2.getCentroid.getY * 100).toInt
        ).z
      })
      .partitionBy(hashPartitioner)
      .flatMap {
        case (_, (id, geom)) =>
          geom match {
            case geomCol: GeoSparkGeometryCollection =>
              val poly = extractPolygons(geomCol)
              List(vector.Feature(poly, id))
            case poly: GeoSparkPolygonal =>
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
                               kwargs: Map[String, Any],
                               spark: SparkSession
                             ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    val featureDF: DataFrame =
      SpatialFeatureDF(input, featureType, kwargs, "geom", spark)

    val spatialFeatureRDD: SpatialRDD[GeoSparkGeometry] =
      Adapter.toSpatialRdd(featureDF, "polyshape")
    spatialFeatureRDD.analyze()

    val envelope: GeoSparkEnvelope = spatialFeatureRDD.boundaryEnvelope

    val spatialGridRDD = GridRDD(envelope, spark)
    val flatJoin: JavaPairRDD[GeoSparkPolygon, GeoSparkGeometry] =
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
      .keyBy({ pair: (GeoSparkPolygon, GeoSparkGeometry) =>
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
      .map { intersection =>
        // use implicit converter to covert to Geotrellis Geometry
        val geotrellisGeom: MultiPolygon = intersection

        geotrellis.vector.Feature(
          geotrellisGeom,
          FeatureIdFactory(featureType)
            .fromUserData(intersection.getUserData.asInstanceOf[String])
        )
      }
  }

}
