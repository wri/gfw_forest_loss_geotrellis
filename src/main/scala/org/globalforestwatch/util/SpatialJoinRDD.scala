package org.globalforestwatch.util

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.JavaPairRDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import java.util

object SpatialJoinRDD {

  def spatialjoin[A <: Geometry, B <: Geometry](
                                                 largerSpatialRDD: SpatialRDD[A],
                                                 smallerSpatialRDD: SpatialRDD[B],
                                                 buildOnSpatialPartitionedRDD: Boolean = true, // Set to TRUE only if run join query
                                                 considerBoundaryIntersection: Boolean = false, // Only return gemeotries fully covered by each query window in queryWindowRDD
                                                 usingIndex: Boolean = false
                                               ): JavaPairRDD[B, util.HashSet[A]] = {


    largerSpatialRDD.spatialPartitioning(GridType.QUADTREE)

    smallerSpatialRDD.spatialPartitioning(largerSpatialRDD.getPartitioner)
    smallerSpatialRDD.buildIndex(
      IndexType.QUADTREE,
      buildOnSpatialPartitionedRDD
    )

    JoinQuery.SpatialJoinQuery(
      largerSpatialRDD,
      smallerSpatialRDD,
      usingIndex,
      considerBoundaryIntersection
    )
  }

  def flatSpatialJoin[A <: Geometry, B <: Geometry](
                                                     largerSpatialRDD: SpatialRDD[A],
                                                     smallerSpatialRDD: SpatialRDD[B],
                                                     buildOnSpatialPartitionedRDD: Boolean = true, // Set to TRUE only if run join query
                                                     considerBoundaryIntersection: Boolean = false, // Only return gemeotries fully covered by each query window in queryWindowRDD
                                                     usingIndex: Boolean = false
                                                   ): JavaPairRDD[B, A] = {

    try {
      largerSpatialRDD.spatialPartitioning(GridType.QUADTREE)
    } catch {
      // Number of partitions cannot be larger than half of total records num
      // This can be an issue for small datasets, so only then we will attempt to count features
      case e: java.lang.IllegalArgumentException => largerSpatialRDD.spatialPartitioning(GridType.QUADTREE, List(1, (largerSpatialRDD.countWithoutDuplicates() / 2).toInt).max)
    }

    smallerSpatialRDD.spatialPartitioning(largerSpatialRDD.getPartitioner)
    smallerSpatialRDD.buildIndex(
      IndexType.QUADTREE,
      buildOnSpatialPartitionedRDD
    )

    JoinQuery.SpatialJoinQueryFlat(
      largerSpatialRDD,
      smallerSpatialRDD,
      usingIndex,
      considerBoundaryIntersection
    )
  }

}
