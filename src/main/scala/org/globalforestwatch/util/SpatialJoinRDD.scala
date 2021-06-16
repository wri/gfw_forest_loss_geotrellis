package org.globalforestwatch.util

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.JavaPairRDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticAnalysis

import java.util
import scala.collection.immutable.HashSet
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

object SpatialJoinRDD {

  def spatialjoin[A <: Geometry : ClassTag, B <: Geometry : ClassTag](
                                                                       queryWindowRDD: SpatialRDD[A],
                                                                       valueRDD: SpatialRDD[B],
                                                                       buildOnSpatialPartitionedRDD: Boolean = true, // Set to TRUE only if run join query
                                                                       considerBoundaryIntersection: Boolean = false, // Only return gemeotries fully covered by each query window in queryWindowRDD
                                                                       usingIndex: Boolean = false
                                                                     ): JavaPairRDD[A, util.HashSet[B]] = {

    try {
      queryWindowRDD.spatialPartitioning(GridType.QUADTREE)

      valueRDD.spatialPartitioning(queryWindowRDD.getPartitioner)

      if (usingIndex)
        queryWindowRDD.buildIndex(
          IndexType.QUADTREE,
          buildOnSpatialPartitionedRDD
        )

      JoinQuery.SpatialJoinQuery(
        valueRDD,
        queryWindowRDD,
        usingIndex,
        considerBoundaryIntersection
      )
    } catch {
      case _: java.lang.IllegalArgumentException =>
        try {
          valueRDD.spatialPartitioning(GridType.QUADTREE)

          queryWindowRDD.spatialPartitioning(valueRDD.getPartitioner)

          if (usingIndex)
            valueRDD.buildIndex(
              IndexType.QUADTREE,
              buildOnSpatialPartitionedRDD
            )

          JoinQuery.SpatialJoinQuery(
            valueRDD,
            queryWindowRDD,
            usingIndex,
            considerBoundaryIntersection
          )
        } catch {
          case _: java.lang.IllegalArgumentException =>
            ForestChangeDiagnosticAnalysis.logger.warn(
              "Skip spatial partitioning. Dataset too small."
            )
            // Use brute force
            bruteForceJoin(queryWindowRDD, valueRDD)

        }
    }
  }

  def flatSpatialJoin[A <: Geometry : ClassTag, B <: Geometry : ClassTag](
                                                                           queryWindowRDD: SpatialRDD[A],
                                                                           valueRDD: SpatialRDD[B],
                                                                           buildOnSpatialPartitionedRDD: Boolean = true, // Set to TRUE only if run join query
                                                                           considerBoundaryIntersection: Boolean = false, // Only return gemeotries fully covered by each query window in queryWindowRDD
                                                                           usingIndex: Boolean = false
                                                                         ): JavaPairRDD[B, A] = {

    val queryWindowCount = queryWindowRDD.approximateTotalCount
    val queryWindowPartitions = queryWindowRDD.rawSpatialRDD.getNumPartitions
    val valueCount = valueRDD.approximateTotalCount
    val valuePartitions = valueRDD.rawSpatialRDD.getNumPartitions

    try {
      queryWindowRDD.spatialPartitioning(
        GridType.QUADTREE,
        Seq(queryWindowPartitions, (queryWindowCount / 2).toInt).min
      )
      valueRDD.spatialPartitioning(queryWindowRDD.getPartitioner)
      if (usingIndex)
        queryWindowRDD.buildIndex(
          IndexType.QUADTREE,
          buildOnSpatialPartitionedRDD
        )

      JoinQuery.SpatialJoinQueryFlat(
        queryWindowRDD,
        valueRDD,
        usingIndex,
        considerBoundaryIntersection
      )
    } catch {
      case _: java.lang.IllegalArgumentException =>
        ForestChangeDiagnosticAnalysis.logger.warn(
          "Try to partition using valueRDD."
        )
        try {
          valueRDD.spatialPartitioning(
            GridType.QUADTREE,
            Seq(valuePartitions, (valueCount / 2).toInt).min
          )
          queryWindowRDD.spatialPartitioning(valueRDD.getPartitioner)
          if (usingIndex)
            valueRDD.buildIndex(
              IndexType.QUADTREE,
              buildOnSpatialPartitionedRDD
            )

          JoinQuery.SpatialJoinQueryFlat(
            queryWindowRDD,
            valueRDD,
            usingIndex,
            considerBoundaryIntersection
          )
        } catch {
          case _: java.lang.IllegalArgumentException =>
            ForestChangeDiagnosticAnalysis.logger.warn(
              "Skip spatial partitioning. Dataset too small."
            )
            // need to flip rdd order in order to match response pattern
            bruteForceFlatJoin(valueRDD, queryWindowRDD)

        }

    }
  }

  private def bruteForceJoin[A <: Geometry : ClassTag, B <: Geometry : ClassTag](
                                                                                  leftRDD: SpatialRDD[A],
                                                                                  rightRDD: SpatialRDD[B]
                                                                                ): JavaPairRDD[A, util.HashSet[B]] = {
    JavaPairRDD.fromRDD(
      leftRDD.rawSpatialRDD.rdd
        .cartesian(rightRDD.rawSpatialRDD.rdd)
        .filter((pair: (Geometry, Geometry)) => pair._1.intersects(pair._2))
        .aggregateByKey(new util.HashSet[B]())({ (h: util.HashSet[B], v: B) =>
          h.add(v)
          h
        }, { (left: util.HashSet[B], right: util.HashSet[B]) =>
          left.addAll(right)
          left
        })
    )
  }

  private def bruteForceFlatJoin[A <: Geometry : ClassTag,
    B <: Geometry : ClassTag](
                               leftRDD: SpatialRDD[A],
                               rightRDD: SpatialRDD[B]
                             ): JavaPairRDD[A, B] = {
    JavaPairRDD.fromRDD(
      leftRDD.rawSpatialRDD.rdd
        .cartesian(rightRDD.rawSpatialRDD.rdd)
        .filter((pair: (Geometry, Geometry)) => pair._1.intersects(pair._2))
    )

  }

}
