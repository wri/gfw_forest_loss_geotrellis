package org.globalforestwatch.features

import cats.data.{NonEmptyList, Validated}
import org.apache.log4j.Logger
import geotrellis.store.index.zcurve.Z2
import geotrellis.vector.{Geometry, Feature => GTFeature}
import org.apache.sedona.core.formatMapper.WktReader
import org.apache.spark.HashPartitioner
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter
import org.globalforestwatch.summarystats.{Location, ValidatedLocation}
import org.globalforestwatch.util.{GridRDD, SpatialJoinRDD}
import org.globalforestwatch.util.IntersectGeometry.validatedIntersection
import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.operation.union.UnaryUnionOp

import java.util.Collection

object ValidatedFeatureRDD {
  val logger = Logger.getLogger("FeatureRDD")

  /**
   * Reads features from source and optionally splits them by 1x1 degree grid.
   * - If the feature WKB is invalid, the feature will be dropped
   * - If there is a problem with intersection logic, the erroring feature id will propagate to output
   */
  def apply(
    input: NonEmptyList[String],
    featureType: String,
    filters: FeatureFilter,
    splitFeatures: Boolean
  )(implicit spark: SparkSession): RDD[ValidatedLocation[Geometry]] = {

    if (splitFeatures) {
      val featureObj: Feature = Feature(featureType)
      val featureDF: DataFrame = SpatialFeatureDF.applyValidated(input, featureObj, filters, "geom", spark)
      splitGeometries(featureType, featureDF, spark)
    } else {
      val featureObj: Feature = Feature(featureType)
      val featuresDF: DataFrame = FeatureDF(input, featureObj, filters, spark)

      featuresDF.rdd
        .mapPartitions(
          { iter: Iterator[Row] =>
            for {
              i <- iter
              if featureObj.isNonEmptyGeom(i)
            } yield {
              val GTFeature(geom, id) = featureObj.get(i)
              Validated.Valid(Location(id, geom))
            }
          },
          preservesPartitioning = true
        )
    }
  }

  private def splitGeometries(
                               featureType: String,
                               featureDF: DataFrame,
                               spark: SparkSession
                             ): RDD[ValidatedLocation[Geometry]] = {
    val spatialFeatureRDD: SpatialRDD[Geometry] = Adapter.toSpatialRdd(featureDF, "polyshape")
    spatialFeatureRDD.analyze()

    spatialFeatureRDD.rawSpatialRDD = spatialFeatureRDD.rawSpatialRDD.rdd.map { geom: Geometry =>
      val featureId = FeatureId.fromUserData(featureType, geom.getUserData.asInstanceOf[String], delimiter = ",")
      geom.setUserData(featureId)
      geom
    }

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
      .flatMap { case (_, (gridCell, geom)) =>
        val fid = geom.getUserData.asInstanceOf[FeatureId]
        validatedIntersection(geom, gridCell)
          .leftMap { err => Location(fid, err) }
          .map { geoms => geoms.map { geom =>
            // val gtGeom: Geometry = toGeotrellisGeometry(geom)
            Location(fid, geom)
          } }
          .traverse(identity)
      }
  }
}
