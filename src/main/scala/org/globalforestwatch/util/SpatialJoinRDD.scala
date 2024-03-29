package org.globalforestwatch.util

import org.locationtech.jts.geom.Geometry
import org.apache.spark.api.java.JavaPairRDD
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticAnalysis

import java.util
import scala.reflect.ClassTag

object SpatialJoinRDD {

  def spatialjoin[A <: Geometry : ClassTag, B <: Geometry : ClassTag](
                                                                       queryWindowRDD: SpatialRDD[A],
                                                                       valueRDD: SpatialRDD[B],
                                                                       buildOnSpatialPartitionedRDD: Boolean = true, // Set to TRUE only if run join query
                                                                       considerBoundaryIntersection: Boolean = false, // Only return gemeotries fully covered by each query window in queryWindowRDD
                                                                       usingIndex: Boolean = false
                                                                     ): JavaPairRDD[A, util.List[B]] = {

    try {
      queryWindowRDD.spatialPartitioning(GridType.KDBTREE)

      valueRDD.spatialPartitioning(queryWindowRDD.getPartitioner)

      if (usingIndex)
        queryWindowRDD.buildIndex(
          IndexType.RTREE,
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
          valueRDD.spatialPartitioning(GridType.KDBTREE)

          queryWindowRDD.spatialPartitioning(valueRDD.getPartitioner)

          if (usingIndex)
            valueRDD.buildIndex(
              IndexType.RTREE,
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
        GridType.KDBTREE,
        Seq(queryWindowPartitions, (queryWindowCount / 2).toInt).min
      )
      valueRDD.spatialPartitioning(queryWindowRDD.getPartitioner)
      if (usingIndex)
        queryWindowRDD.buildIndex(
          IndexType.RTREE,
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
            GridType.KDBTREE,
            Seq(valuePartitions, (valueCount / 2).toInt).min
          )
          queryWindowRDD.spatialPartitioning(valueRDD.getPartitioner)
          if (usingIndex)
            valueRDD.buildIndex(
              IndexType.RTREE,
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
                                                                                ): JavaPairRDD[A, util.List[B]] = {
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
        }).mapValues( hs => new util.ArrayList(hs))
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
