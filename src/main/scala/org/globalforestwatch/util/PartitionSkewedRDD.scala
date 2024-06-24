package org.globalforestwatch.util

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

// Hash partitioner, where we aim to group all rows with same id together and put
// them into the same partition based on hashing. However, if a single id has more
// than maxPartitionSize rows, we break it up into groups of at most maxPartitionSize
// and features in each group go to the same partition. The number of partitions is
// the max of rdd.sparkContext.defaultParallelism, or the number of id groups (which
// is the number of unique ids, plus extra groups because of ids that have more than
// maxPartitionSize rows).
object RepartitionSkewedRDD {
  def bySparseId[A: ClassTag](rdd: RDD[(Long, A)], maxPartitionSize: Int): RDD[A] = {
    val counts = rdd.map{ case (id, _) => (id, 1l) }.reduceByKey(_ + _).collect().sortBy(_._2)
    val splits = PartitionSplit.fromCounts(counts, maxPartitionSize)
    val partitionIndex: (Long, A) => Int = (id, v) => splits(id).partitionForRow(v)
    val parallelism = rdd.sparkContext.defaultParallelism
    val numPartitions: Int = math.max(parallelism, splits.size / 16).toInt

    rdd
      .map { case (id, v) => (partitionIndex(id, v), v) }
      .partitionBy(new HashPartitioner(numPartitions))
      .values
  }
}


case class PartitionSplit(partitionIndex: Int,  splits: Int) {
  require(splits >= 0, s"Min of one split required: $splits")

  def maxPartitionIndex: Int = partitionIndex + splits - 1

  def partitionForRow[A](row: A): Int = {
    partitionIndex + row.hashCode() % splits
  }
}

object PartitionSplit {
  def fromCounts(counts: Seq[(Long, Long)], maxPartitionSize: Int): Map[Long, PartitionSplit] = {
    counts.foldLeft(List.empty[(Long, PartitionSplit)]) {
      case (Nil, (id, count)) =>
        val index = 0
        val splits = math.ceil(count.toDouble / maxPartitionSize).toInt
        id -> PartitionSplit(index, splits) :: Nil

      case (acc@((_, head) :: _), (id, count)) =>
        val index = head.partitionIndex + head.splits
        val splits = math.ceil(count.toDouble / maxPartitionSize).toInt
        id -> PartitionSplit(index, splits) :: acc
    }.toMap
  }
}
