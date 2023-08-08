package org.globalforestwatch.summarystats.afi

import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataLossYearly
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataLossYearlyCategory
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDoubleCategory

import cats.Semigroup
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class AFiData(
  /** Annual Tree Cover Loss on Natural Forest pixels within location geometry */
  tree_cover_loss_natural_forest_yearly: AFiDataLossYearly,
  /** Natural Forest extent within location geometry */
  natural_forest_extent: AFiDataDouble,
  is_negligible_risk: AFiDataBoolean,
  negligible_risk_area: AFiDataDoubleCategory,
  total_area: AFiDataDoubleCategory,
) {
  def merge(other: AFiData): AFiData = {
    AFiData(
      tree_cover_loss_natural_forest_yearly.merge(other.tree_cover_loss_natural_forest_yearly),
      natural_forest_extent.merge(other.natural_forest_extent),
      is_negligible_risk.merge(other.is_negligible_risk),
      negligible_risk_area.merge(other.negligible_risk_area),
      total_area.merge(other.total_area)
    )
  }
}

object AFiData {

  def empty: AFiData =
    AFiData(
      AFiDataLossYearly.empty,
      AFiDataDouble.empty,
      AFiDataBoolean.empty
    )

  implicit val afiDataSemigroup: Semigroup[AFiData] =
    new Semigroup[AFiData] {
      def combine(x: AFiData, y: AFiData): AFiData =
        x.merge(y)
    }

  implicit def dataExpressionEncoder: ExpressionEncoder[AFiData] =
    frameless
      .TypedExpressionEncoder[AFiData]
      .asInstanceOf[ExpressionEncoder[AFiData]]
}