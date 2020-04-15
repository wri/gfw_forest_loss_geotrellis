package org.globalforestwatch.features

import java.util.HashSet

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.{Geometry, Point}
import geotrellis.vector
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{PointRDD, SpatialRDD}
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.util.GeometryReducer
import scala.collection.JavaConverters._

object PointInPolygonFeatureRDD {
  def apply(polygonFeatureUris: NonEmptyList[String],
            polygonFeatureObj: Feature,
            pointFeatureUris: NonEmptyList[String],
            pointFeatureObj: Feature,
            kwargs: Map[String, Any],
            spark: SparkSession): RDD[vector.Feature[vector.Geometry, FeatureId]] = {

    val pointFeatureDF = FeatureDF(pointFeatureUris, pointFeatureObj, kwargs, spark, "longitude", "latitude")
    var pointFeatureRDD = new PointRDD
    pointFeatureRDD.rawSpatialRDD = Adapter.toJavaRdd(pointFeatureDF).asInstanceOf[JavaRDD[Point]]

    val polyFeatureDF = FeatureDF(polygonFeatureUris, polygonFeatureObj, kwargs, spark, "geom")
    var polyFeatureRDD = new SpatialRDD[Geometry]
    polyFeatureRDD.rawSpatialRDD = Adapter.toJavaRdd(polyFeatureDF)

    val considerBoundaryIntersection = false // Only return geometries fully covered by each query window in queryWindowRDD

    pointFeatureRDD.analyze()

    pointFeatureRDD.spatialPartitioning(GridType.QUADTREE)
    polyFeatureRDD.spatialPartitioning(pointFeatureRDD.getPartitioner)

    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    pointFeatureRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

    val joinResultRDD: org.apache.spark.api.java.JavaPairRDD[Geometry, HashSet[Point]] = JoinQuery.SpatialJoinQuery(pointFeatureRDD, polyFeatureRDD, true, considerBoundaryIntersection)
    val joinResultScalaRDD: RDD[(Geometry, HashSet[Point])] = org.apache.spark.api.java.JavaPairRDD.toRDD(joinResultRDD)

    joinResultScalaRDD.flatMap {
      case (geom: Geometry, points: HashSet[Point]) =>
        val geomFields = geom.getUserData.asInstanceOf[String].split('\t')
        val polyFeatureId = polygonFeatureObj.getFeatureId(geomFields)

        points.asScala.map((pt: Point) => {
          val pointFeatureData = pt.getUserData.asInstanceOf[String].split('\t')

          val geom = GeometryReducer.reduce(GeometryReducer.gpr)(
            vector.Point(pt.getX, pt.getY)
          )

          val pointFeatureId = pointFeatureObj.getFeatureId(pointFeatureData)
          vector.Feature(geom, CombinedFeatureId(pointFeatureId, polyFeatureId))
        })
    }
  }
}
