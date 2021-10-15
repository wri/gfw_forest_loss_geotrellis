package org.globalforestwatch.features

import cats.data.{NonEmptyList, Validated}
import com.vividsolutions.jts.geom.{
  Envelope => GeoSparkEnvelope, Geometry => GeoSparkGeometry, Point => GeoSparkPoint, Polygon => GeoSparkPolygon,
  MultiPolygon => GeoSparkMultiPolygon, Polygonal => GeoSparkPolygonal, GeometryCollection => GeoSparkGeometryCollection
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
import org.globalforestwatch.summarystats.{GeometryError, ValidatedRow}
import org.globalforestwatch.util.{GridRDD, SpatialJoinRDD}
import org.globalforestwatch.util.IntersectGeometry.{extractPolygons, validatedIntersection}
import org.globalforestwatch.util.Util.getAnyMapValue
import org.globalforestwatch.util.ImplicitGeometryConverter._

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
    splitFeatures: Boolean,
    spark: SparkSession
  ): RDD[(FeatureId, ValidatedRow[geotrellis.vector.Feature[Geometry, FeatureId]])] = {

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
              val feat = featureObj.get(i)
              (feat.data, Validated.Valid(feat))
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
  ): RDD[(FeatureId, ValidatedRow[geotrellis.vector.Feature[Geometry, FeatureId]])] = {

    val spatialFeatureRDD: SpatialRDD[GeoSparkGeometry] = Adapter.toSpatialRdd(featureDF, "polyshape")
    spatialFeatureRDD.analyze()

    spatialFeatureRDD.rawSpatialRDD = spatialFeatureRDD.rawSpatialRDD.rdd.map { geom: GeoSparkGeometry =>
      val featureId = FeatureId.fromUserData(featureType, geom.getUserData.asInstanceOf[String], delimiter = ",")
      geom.setUserData(featureId)
      geom
    }

    val envelope: GeoSparkEnvelope = spatialFeatureRDD.boundaryEnvelope
    val spatialGridRDD = GridRDD(envelope, spark, clip = true)
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
      .flatMap { case (_, (gridCell, geom)) =>
        val (fid, geometries) = validatedIntersection(geom, gridCell)
        geometries.traverse { geoms => geoms }.map { vg => (fid.asInstanceOf[FeatureId], vg) }
      }
      .flatMap {
        case (_, Validated.Valid(mp)) if mp.isEmpty =>
          // This is Valid result of intersection, but with no area == not an intersection
          None // flatMap will drop the None,
        case (fid, validated) =>
          val validatedFeature = validated.map { intersection =>
            val geotrellisGeom: MultiPolygon = intersection
            geotrellis.vector.Feature(geotrellisGeom, fid)
          }
          Some(fid -> validatedFeature)
      }
  }
}
