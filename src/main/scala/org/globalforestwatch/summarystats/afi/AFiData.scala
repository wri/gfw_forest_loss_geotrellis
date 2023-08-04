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
  /** Annual Tree Cover Loss TCD 30 within location geometry */
  tree_cover_loss_total_yearly: ForestChangeDiagnosticDataLossYearly,
  /** Annual Tree Cover Loss on Natural Forest pixels within location geometry */
  tree_cover_loss_natural_forest_yearly: ForestChangeDiagnosticDataLossYearlyCategory,
  /** Natural Forest extent within location geometry */
  natural_forest_extent: ForestChangeDiagnosticDataDoubleCategory
) {

  def merge(other: AFiData): AFiData = {
    AFiData(
      tree_cover_loss_total_yearly.merge(other.tree_cover_loss_total_yearly),
      tree_cover_loss_natural_forest_yearly.merge(other.tree_cover_loss_natural_forest_yearly),
      natural_forest_extent.merge(other.natural_forest_extent)
    )
  }
}

object AFiData {

  def empty: AFiData =
    AFiData(
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearlyCategory.empty,
      ForestChangeDiagnosticDataDoubleCategory.empty
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
