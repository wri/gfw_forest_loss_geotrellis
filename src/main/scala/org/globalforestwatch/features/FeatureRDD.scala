package org.globalforestwatch.features

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.{Envelope, Geometry => GeoSparkGeometry, Point => GeoSparkPoint}
import geotrellis.vector
import geotrellis.vector.Geometry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.util.{GeometryReducer, GridRDD, SpatialJoinRDD}
import org.globalforestwatch.util.IntersectGeometry.{intersectGeometries, toGeotrellisGeometry}
import org.globalforestwatch.util.Util.getAnyMapValue

object FeatureRDD {
  def apply(
             input: NonEmptyList[String],
             featureType: String,
             kwargs: Map[String, Any],
             spark: SparkSession
           ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    val splitFeatures = getAnyMapValue[Boolean](kwargs, "splitFeatures")
    //    val geomFieldName = getAnyMapValue[String](kwargs, "geomFieldName")

    if (splitFeatures) splitGeometries(input, featureType, kwargs, spark)
    else {
      val featureObj: Feature = FeatureFactory(featureType).featureObj
      val featuresDF: DataFrame =
        FeatureDF(input, featureObj, kwargs, spark)

      val splitFeatures = getAnyMapValue[Boolean](kwargs, "splitFeatures")

      featuresDF.rdd
        .mapPartitions({ iter: Iterator[Row] =>
          for {
            i <- iter
            if splitFeatures || featureObj.isValidGeom(i)
          } yield {
            featureObj.get(i)
          }
        }, preservesPartitioning = true)

    }

  }

  def apply(
             featureObj: Feature,
             spatialRDD: SpatialRDD[GeoSparkGeometry],
             kwargs: Map[String, Any]
           ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    val scalaRDD =
      org.apache.spark.api.java.JavaRDD.toRDD(spatialRDD.spatialPartitionedRDD)

    scalaRDD
      .map {
        case pt: GeoSparkPoint =>
          val pointFeatureData = pt.getUserData.asInstanceOf[String].split('\t')

          val geom = GeometryReducer.reduce(GeometryReducer.gpr)(
            vector.Point(pt.getX, pt.getY)
          )

          val pointFeatureId: FeatureId =
            featureObj.getFeatureId(pointFeatureData)
          vector.Feature(geom, pointFeatureId)
        case _ =>
          throw new NotImplementedError(
            "Cannot convert geometry type to Geotrellis RDD"
          )
      }
    // In case we implement this method for other geometry types we will have to split geometries
    //      .flatMap { feature =>
    //        splitGeometry(feature)
    //      }

  }

  private def splitGeometries(
                               input: NonEmptyList[String],
                               featureType: String,
                               kwargs: Map[String, Any],
                               spark: SparkSession
                             ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    val featureDF: DataFrame =
      SpatialFeatureDF(input, featureType, kwargs, spark, "geom")

    val spatialFeatureRDD: SpatialRDD[GeoSparkGeometry] =
      Adapter.toSpatialRdd(featureDF, "polyshape")
    spatialFeatureRDD.analyze()

    val envelope: Envelope = spatialFeatureRDD.boundaryEnvelope

    val spatialGridRDD = GridRDD(envelope, spark)

    val flatJoin = SpatialJoinRDD.flatSpatialJoin(
      spatialGridRDD,
      spatialFeatureRDD,
      considerBoundaryIntersection = true
    )

    flatJoin.rdd
      .flatMap {
        case (geom, gridCell) =>
          val geometries = intersectGeometries(geom, gridCell)
          geometries
            .map { intersection =>
              val geotrellisGeom = toGeotrellisGeometry(intersection)
              geotrellis.vector.Feature(
                geotrellisGeom,
                FeatureIdFactory(featureType)
                  .fromUserData(geom.getUserData.asInstanceOf[String])
              )
            }

      }
  }

}
