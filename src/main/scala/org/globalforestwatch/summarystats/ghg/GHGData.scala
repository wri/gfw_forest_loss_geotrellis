package org.globalforestwatch.summarystats.ghg

import cats.Semigroup

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Final data for each location.
  */
case class GHGData(
  total_area: GHGDataDouble,
  emissions_factor_yearly: GHGDataValueYearly
) {

  def merge(other: GHGData): GHGData = {

    GHGData(
      total_area.merge(other.total_area),
      emissions_factor_yearly.merge(other.emissions_factor_yearly)
    )
  }
}

object GHGData {

  def empty: GHGData =
    GHGData(
      GHGDataDouble.empty,
      GHGDataValueYearly.empty,
    )

  implicit val lossDataSemigroup: Semigroup[GHGData] =
    new Semigroup[GHGData] {
      def combine(x: GHGData,
                  y: GHGData): GHGData =
        x.merge(y)
    }

  implicit def dataExpressionEncoder: ExpressionEncoder[GHGData] =
    frameless.TypedExpressionEncoder[GHGData]
      .asInstanceOf[ExpressionEncoder[GHGData]]
}
