package org.globalforestwatch.features

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry, Point => GeoSparkPoint, Polygon => GeoSparkPolygon}
import com.vividsolutions.jts.io.WKTWriter
import geotrellis.vector
import geotrellis.vector.{Geometry, Polygon}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.globalforestwatch.util.GeometryReducer
import org.globalforestwatch.util.IntersectGeometry.{getIntersecting1x1Grid, intersectGeometries}
import org.globalforestwatch.util.Util.getAnyMapValue
import org.locationtech.jts.io.WKTReader

import scala.annotation.tailrec

object FeatureRDD {
  def apply(
             input: NonEmptyList[String],
             featureObj: Feature,
             kwargs: Map[String, Any],
             spark: SparkSession,
           ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {
    val featuresDF: DataFrame =
      FeatureDF(input, featureObj, kwargs, spark)

    val splitFeatures = getAnyMapValue[Boolean](kwargs, "splitFeatures")

    val featureRdd = featuresDF.rdd
      .mapPartitions({ iter: Iterator[Row] =>
        for {
          i <- iter
          if splitFeatures || featureObj.isValidGeom(i)
        } yield {
          featureObj.get(i)
        }
      }, preservesPartitioning = true)

    if (splitFeatures) featureRdd.flatMap { feature =>
      splitGeometry(feature)
    } else featureRdd
  }

  def apply(
             featureObj: Feature,
             spatialRDD: SpatialRDD[GeoSparkGeometry],
             kwargs: Map[String, Any] ,
             feature2Obj: Option[Feature] = None,
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
        case shp: GeoSparkPolygon =>
          val featureData = shp.getUserData.asInstanceOf[String].split('\t')
          val writer: WKTWriter = new WKTWriter()
          val wkt = writer.write(shp)

          val reader: WKTReader = new WKTReader()
          val geom = GeometryReducer.reduce(GeometryReducer.gpr)(
            reader.read(wkt)
          )

          val feature1Data = featureData.head.drop(1).dropRight(1).split(',')
          val feature1Id: FeatureId =
            featureObj.getFeatureId(feature1Data, parsed=true)

          val feature2Id: FeatureId =
            feature2Obj match {
              case Some(obj) =>
                val feature2Data = featureData.tail.head.drop(1).dropRight(1).split(',')
                obj.getFeatureId(feature2Data, parsed=true)
              case None =>
                throw new IllegalArgumentException("Must have two feature objects for polygon-polygon intersections.")
            }

          // TODO do we need to split the geometries always here?
          vector.Feature(geom, CombinedFeatureId(feature1Id, feature2Id))
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

  private def splitGeometry(
                             feature: geotrellis.vector.Feature[Geometry, FeatureId]
                           ): List[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    @tailrec def loop(geom: Geometry,
                      gridGeoms: IndexedSeq[Polygon],
                      acc: List[Geometry]): List[Geometry] = {
      if (gridGeoms.isEmpty) acc
      else {

        val gridGeom = gridGeoms.head;

        val intersections: List[Geometry] =
          intersectGeometries(feature.geom, gridGeom)

        loop(geom, gridGeoms.tail, acc ::: intersections)
      }
    }

    val gridGeoms = getIntersecting1x1Grid(feature.geom)

    loop(feature.geom, gridGeoms, List()).map(vector.Feature(_, feature.data))

  }

}
