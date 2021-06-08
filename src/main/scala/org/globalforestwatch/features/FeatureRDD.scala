package org.globalforestwatch.features

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.{
  Envelope => GeoSparkEnvelope,
  Geometry => GeoSparkGeometry,
  Point => GeoSparkPoint,
  Polygon => GeoSparkPolygon,
  Polygonal => GeoSparkPolygonal
}
import org.apache.log4j.Logger
import com.vividsolutions.jts.io.WKTWriter
import geotrellis.vector
import geotrellis.vector.{Geometry, MultiPolygon}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.util.{
  GeotrellisGeometryReducer,
  GridRDD,
  SpatialJoinRDD
}
import org.globalforestwatch.util.IntersectGeometry.intersectGeometries
import org.globalforestwatch.util.Util.getAnyMapValue
import org.locationtech.jts.io.WKTReader
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
  def apply(featureObj: Feature,
            spatialRDD: SpatialRDD[GeoSparkGeometry],
            kwargs: Map[String, Any],
           ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    val scalaRDD =
      org.apache.spark.api.java.JavaRDD.toRDD(spatialRDD.spatialPartitionedRDD)

    scalaRDD
      .map {
        case pt: GeoSparkPoint =>
          val pointFeatureData = pt.getUserData.asInstanceOf[String].split('\t')

          val geom = GeotrellisGeometryReducer.reduce(
            GeotrellisGeometryReducer.gpr
          )(vector.Point(pt.getX, pt.getY))

          val pointFeatureId: FeatureId =
            featureObj.getFeatureId(pointFeatureData)
          vector.Feature(geom, pointFeatureId)
        case _ =>
          throw new IllegalArgumentException(
            "Point-in-polygon intersection must be points."
          )
      }

    // In case we implement this method for other geometry types we will have to split geometries
    //      .flatMap { feature =>
    //        splitGeometry(feature)
    //      }

  }

  /*
    Convert polygon-polygon intersection join to feature RDD
   */
  def apply(feature1Obj: Feature,
            feature2Obj: Feature,
            spatialRDD: SpatialRDD[GeoSparkGeometry],
            kwargs: Map[String, Any],
           ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {
    val scalaRDD =
      org.apache.spark.api.java.JavaRDD.toRDD(spatialRDD.spatialPartitionedRDD)

    scalaRDD
      .flatMap {
        case shp: GeoSparkPolygonal =>
          val featureData = shp.getUserData.asInstanceOf[String].split('\t')
          val writer: WKTWriter = new WKTWriter()
          val wkt = writer.write(shp)

          val reader: WKTReader = new WKTReader()
          val geom =
            GeotrellisGeometryReducer.reduce(GeotrellisGeometryReducer.gpr)(
              reader.read(wkt)
            )

          val feature1Data = featureData.head.drop(1).dropRight(1).split(',')
          val feature1Id: FeatureId =
            feature1Obj.getFeatureId(feature1Data, parsed = true)

          val feature2Data =
            featureData.tail.head.drop(1).dropRight(1).split(',')
          val feature2Id: FeatureId =
            feature2Obj.getFeatureId(feature2Data, parsed = true)

          List(vector.Feature(geom, CombinedFeatureId(feature1Id, feature2Id)))
        case _ =>
          // Polygon-polygon intersections can generate points or lines, which we just want to ignore
          logger.warn("Cannot process geometry type")
          List()
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
    val flatJoin: JavaPairRDD[GeoSparkGeometry, GeoSparkPolygon] =
      SpatialJoinRDD.flatSpatialJoin(
        spatialGridRDD,
        spatialFeatureRDD,
        considerBoundaryIntersection = true
      )

    flatJoin.rdd
      .flatMap {
        case (geom, gridCell) =>
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
