package org.globalforestwatch.util

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object RepartitionSkewedRDD {
  def bySparseId[A: ClassTag](rdd: RDD[(Long, A)], maxPartitionSize: Int): RDD[A] = {
    val counts = rdd.map{ case (id, _) => (id, 1l) }.reduceByKey(_ + _).collect().sortBy(_._2)
    val totalCount = counts.map(_._2).sum
    val splits = PartitionSplit.fromCounts(counts, maxPartitionSize)
    val paritionIndex: (Long, A) => Int = (id, v) => splits(id).partitionForRow(v)
    val numPartitions: Int = math.min((totalCount / 1024).toInt, 2000)
    println(s"numPartitions: ${numPartitions}")

    rdd
      .map { case (id, v) => (paritionIndex(id, v), v) }
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