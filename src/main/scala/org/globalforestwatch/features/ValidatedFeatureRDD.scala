package org.globalforestwatch.features

import cats.data.{NonEmptyList, Validated}
import com.vividsolutions.jts.geom.{
  Envelope => GeoSparkEnvelope,
  Geometry => GeoSparkGeometry,
  Point => GeoSparkPoint,
  Polygon => GeoSparkPolygon,
  MultiPolygon => GeoSparkMultiPolygon,
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
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PolygonRDD}
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.summarystats.{JobError, NoIntersectionError, ValidatedRow}
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
    val spatialGridRDD = if (envelope != null) GridRDD(envelope, spark, clip = true) else new PolygonRDD(spark.sparkContext.emptyRDD[GeoSparkPolygon])

    val matchedGeoms: RDD[(FeatureId, ValidatedRow[geotrellis.vector.Feature[Geometry, FeatureId]])] =
      joinToGrid(spatialFeatureRDD, spatialGridRDD, spark, true)
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

    // val flatJoin: JavaPairRDD[GeoSparkPolygon, GeoSparkGeometry] =
    //   SpatialJoinRDD.flatSpatialJoin(
    //     spatialFeatureRDD,
    //     spatialGridRDD,
    //     considerBoundaryIntersection = true
    //   )

    /*
      partitions will come back very skewed and we will need to even them out for any downstream analysis
      For the summary analysis we will eventually use a range partitioner.
      However, the range partitioner uses sampling to come up with the  break points for the different partitions.
      If the input RDD is already heavily skewed, sampling will be off and the range partitioner won't do a good job.
     */
    // val hashPartitioner = new HashPartitioner(flatJoin.getNumPartitions)

    // val matchedGeoms: RDD[(FeatureId, ValidatedRow[geotrellis.vector.Feature[Geometry, FeatureId]])] = flatJoin.rdd
    //   .keyBy({ pair: (GeoSparkPolygon, GeoSparkGeometry) =>
    //     Z2(
    //       (pair._1.getCentroid.getX * 100).toInt,
    //       (pair._1.getCentroid.getY * 100).toInt
    //     ).z
    //   })
    //   .partitionBy(hashPartitioner)
    //   .flatMap { case (_, (gridCell, geom)) =>
    //     val (fid, geometries) = validatedIntersection(geom, gridCell)
    //     geometries.traverse { geoms => geoms }.map { vg => (fid.asInstanceOf[FeatureId], vg) }
    //   }
    //   .flatMap {
    //     case (_, Validated.Valid(mp)) if mp.isEmpty =>
    //       // This is Valid result of intersection, but with no area == not an intersection
    //       None // flatMap will drop the None,
    //     case (fid, validated) =>
    //       val validatedFeature = validated.map { intersection =>
    //         val geotrellisGeom: MultiPolygon = intersection
    //         geotrellis.vector.Feature(geotrellisGeom, fid)
    //       }
    //       Some(fid -> validatedFeature)
    //   }

    val complementGridRDD = GridRDD.complement(envelope, spark)
    val droppedGeoms: RDD[(FeatureId, ValidatedRow[geotrellis.vector.Feature[Geometry, FeatureId]])] =
      joinToGrid(spatialFeatureRDD, complementGridRDD, spark)
        .flatMap {
          case (_, Validated.Valid(mp)) if mp.isEmpty =>
            // Valid result of intersection with no area => not an intersection
            None // flatMap will drop the None
          case (fid, Validated.Valid(_)) =>
            // There was a proper intersection; log the geometry as an out of bounds error
            Some(fid)
          case (fid, Validated.Invalid(_)) =>
            // There was an intersection, but there was a JTS error in computing it; ignore
            None
        }
        .distinct
        .map{ fid => (fid, Validated.invalid[JobError, geotrellis.vector.Feature[Geometry, FeatureId]](NoIntersectionError)) }

      // if (complementGridRDD.approximateTotalCount > 0) {
      //   // look for features entirely outside of the tree cover grid area
      //   val complementJoin: JavaPairRDD[GeoSparkPolygon, GeoSparkGeometry] =
      //     SpatialJoinRDD.flatSpatialJoin(
      //       spatialFeatureRDD,
      //       complementGridRDD,
      //       considerBoundaryIntersection = false
      //     )
      //   val cHashPartitioner = new HashPartitioner(complementJoin.getNumPartitions)
      //   complementJoin.rdd
      //     .keyBy({ pair: (GeoSparkPolygon, GeoSparkGeometry) =>
      //       Z2(
      //         (pair._1.getCentroid.getX * 100).toInt,
      //         (pair._1.getCentroid.getY * 100).toInt
      //       ).z
      //     })
      //     .partitionBy(cHashPartitioner)
      //     .flatMap { case (_, (gridCell, geom)) =>
      //       val (fid, geometries) = validatedIntersection(geom, gridCell)
      //       geometries.traverse { geoms => geoms }.map { vg => (fid.asInstanceOf[FeatureId], vg) }
      //     }
      //     .flatMap {
      //       case (_, Validated.Valid(mp)) if mp.isEmpty =>
      //         // Valid result of intersection with no area => not an intersection
      //         None // flatMap will drop the None
      //       case (fid, Validated.Valid(_)) =>
      //         // There was a proper intersection; log the geometry as an out of bounds error
      //         Some(fid)
      //       case (fid, Validated.Invalid(_)) =>
      //         // There was an intersection, but there was a JTS error in computing it; ignore
      //         None
      //     }
      //     .distinct
      //     .map{ fid => (fid, Validated.invalid[JobError, geotrellis.vector.Feature[Geometry, FeatureId]](NoIntersectionError)) }
      // } else {
      //   spark.sparkContext.emptyRDD[(FeatureId, ValidatedRow[geotrellis.vector.Feature[Geometry, FeatureId]])]
      // }

    matchedGeoms.union(droppedGeoms)
  }

  private def joinToGrid(
    spatialFeatureRdd: SpatialRDD[GeoSparkGeometry],
    gridRdd: PolygonRDD,
    spark: SparkSession,
    considerBoundaryIntersection: Boolean = false
  ): RDD[(FeatureId, ValidatedRow[GeoSparkMultiPolygon])] = {
    if (gridRdd.approximateTotalCount > 0) {
      // look for features entirely outside of the tree cover grid area
      val joined: JavaPairRDD[GeoSparkPolygon, GeoSparkGeometry] =
        SpatialJoinRDD.flatSpatialJoin(
          spatialFeatureRdd,
          gridRdd,
          considerBoundaryIntersection
        )
      val hashPartitioner = new HashPartitioner(joined.getNumPartitions)
      joined.rdd
        .keyBy({ pair: (GeoSparkPolygon, GeoSparkGeometry) =>
          Z2(
            (pair._1.getCentroid.getX * 100).toInt,
            (pair._1.getCentroid.getY * 100).toInt
          ).z
        })
      .partitionBy(hashPartitioner)
      .flatMap { case (_, (gridCell, geom)) =>
        val (fid, geometries) = validatedIntersection(geom, gridCell)
        geometries.traverse { geoms => geoms }.map {
          vg => (fid.asInstanceOf[FeatureId], vg)
        }
      }
    } else {
      spark.sparkContext.emptyRDD
    }
  }
}
