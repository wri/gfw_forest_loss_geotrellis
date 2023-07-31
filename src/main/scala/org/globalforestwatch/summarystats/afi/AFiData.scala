package org.globalforestwatch.summarystats.afi

import cats.Semigroup
import frameless.TypedEncoder.bigDecimalEncoder

import scala.collection.immutable.SortedMap
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class AFiData(
  treeCoverLoss: AFiDataLossYearly
) {

  def merge(other: AFiData): AFiData = {
    AFiData(
      treeCoverLoss.merge(other.treeCoverLoss)
    )
  }
}

object AFiData {

  def empty: AFiData =
    AFiData(AFiDataLossYearly.empty)

  implicit val afiDataSemigroup: Semigroup[AFiData] =
    new Semigroup[AFiData] {
      def combine(x: AFiData, y: AFiData): AFiData =
        x.merge(y)
    }

  implicit def dataExpressionEncoder: ExpressionEncoder[AFiData] =
    frameless.TypedExpressionEncoder[AFiData]
      .asInstanceOf[ExpressionEncoder[AFiData]]
}
