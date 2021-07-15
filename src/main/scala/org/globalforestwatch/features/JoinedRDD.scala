package org.globalforestwatch.features

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.JavaPairRDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.SpatialRDD

import java.util

object JoinedRDD {

  def apply(
             fireAlertSpatialRDD: SpatialRDD[Geometry],
             featureSpatialRDD: SpatialRDD[Geometry]
           ): JavaPairRDD[Geometry, util.HashSet[Geometry]] = {

    // Join Fire and Feature RDD
    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    val considerBoundaryIntersection = false // Only return gemeotries fully covered by each query window in queryWindowRDD
    val usingIndex = false

    fireAlertSpatialRDD.spatialPartitioning(GridType.QUADTREE)

    featureSpatialRDD.spatialPartitioning(fireAlertSpatialRDD.getPartitioner)
    featureSpatialRDD.buildIndex(
      IndexType.QUADTREE,
      buildOnSpatialPartitionedRDD
    )

    JoinQuery.SpatialJoinQuery(
      fireAlertSpatialRDD,
      featureSpatialRDD,
      usingIndex,
      considerBoundaryIntersection
    )
  }
}
