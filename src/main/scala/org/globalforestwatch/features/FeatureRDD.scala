package org.globalforestwatch.features

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.{
  Envelope,
  MultiPolygon,
  Polygon,
  Geometry => GeoSparkGeometry,
  Point => GeoSparkPoint,
  Polygonal => GeoSparkPolygonal
}
import org.apache.log4j.Logger
import com.vividsolutions.jts.io.WKTWriter
import geotrellis.vector
import geotrellis.vector.Geometry
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialRDD.{PolygonRDD, SpatialRDD}
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.util.{
  GeometryReducer,
  GridDF,
  GridRDD,
  SpatialJoinRDD
}
import org.globalforestwatch.util.IntersectGeometry.{
  intersectGeometries,
  toGeotrellisGeometry
}
import org.globalforestwatch.util.Util.{
  countRecordsPerPartition,
  getAnyMapValue
}
import org.locationtech.jts.io.WKTReader

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

          val geom = GeometryReducer.reduce(GeometryReducer.gpr)(
            vector.Point(pt.getX, pt.getY)
          )

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
            GeometryReducer.reduce(GeometryReducer.gpr)(reader.read(wkt))

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

    val envelope: Envelope = spatialFeatureRDD.boundaryEnvelope

    //    val gridDF = GridDF(envelope, spark)
    //
    //    featureDF.show(5)
    //
    //    gridDF.show(5)
    //
    //
    //    val intersectionRDD = PolygonIntersectionRDD(featureDF, gridDF, spark, kwargs)
    //
    //    // TODO: refactor to remove duplicate code
    //    intersectionRDD.analyze()
    //    intersectionRDD.spatialPartitioning(GridType.QUADTREE)
    //    val scalaRDD =
    //      org.apache.spark.api.java.JavaRDD.toRDD(intersectionRDD.spatialPartitionedRDD)
    //
    //    scalaRDD.flatMap {
    //      case shp: GeoSparkPolygonal =>
    //        val featureData = shp.getUserData.asInstanceOf[String].split('\t')
    //        val geom = toGeotrellisGeometry(shp)
    //        val feature1Data = featureData.head.drop(1).dropRight(1).split(',')
    //        val featureObj: Feature = FeatureFactory(featureType).featureObj
    //        val featureId: FeatureId =
    //          featureObj.getFeatureId(feature1Data, parsed = true)
    //
    //        Some(vector.Feature(geom, featureId))
    //      case _ =>
    //        // Polygon-polygon intersections can generate points or lines, which we just want to ignore
    //        logger.warn(
    //          "Cannot process geometry type"
    //        )
    //        None
    //    }

    val spatialGridRDD = GridRDD(envelope, spark)
    val flatJoin: JavaPairRDD[GeoSparkGeometry, Polygon] =
      SpatialJoinRDD.flatSpatialJoin(
        spatialGridRDD,
        spatialFeatureRDD,
        considerBoundaryIntersection = true
      )

    val intersectionRDD = flatJoin.rdd.flatMap {
      case (geom, gridCell) =>
        val geometries = intersectGeometries(geom, gridCell)
        geometries
    }.toJavaRDD

    val polygonRDD = new PolygonRDD(intersectionRDD)
    polygonRDD.analyze()
    //    List(1, (polygonRDD.countWithoutDuplicates / 2).toInt).max
    polygonRDD.spatialPartitioning(GridType.RTREE, List(1, (polygonRDD.countWithoutDuplicates / 2).toInt).max)

    countRecordsPerPartition(polygonRDD.rawSpatialRDD, spark)

    val finalRDD = polygonRDD.rawSpatialRDD
      .map { intersection =>
        val geotrellisGeom = toGeotrellisGeometry(intersection)
        geotrellis.vector.Feature(
          geotrellisGeom,
          FeatureIdFactory(featureType)
            .fromUserData(intersection.getUserData.asInstanceOf[String])
        )
      }

    countRecordsPerPartition(finalRDD, spark)
    finalRDD

  }

}
