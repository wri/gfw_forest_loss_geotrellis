package org.globalforestwatch.summarystats.ghg

import cats.Semigroup

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Final data for each location.
  */
case class GHGData(
  total_area: GHGDataDouble,  // in hectares
  production: GHGDataDouble,  // in Mg (metric tonnes)

  emissions_co2_yearly: GHGDataValueYearly,
  emissions_ch4_yearly: GHGDataValueYearly,
  emissions_n2o_yearly: GHGDataValueYearly,
  emissions_yearly: GHGDataValueYearly,

  // Before prodDivide, these fields are emissions in Mg CO2e.
  // After prodDivide, these fields are the emissions factors, Mg CO2e / Mg yield.
  ef_co2_yearly: GHGDataValueYearly,
  ef_ch4_yearly: GHGDataValueYearly,
  ef_n2o_yearly: GHGDataValueYearly,
  emissions_factor_yearly: GHGDataValueYearly
) {

  // Divide the emissions fields by production after all aggregation, so they
  // become actual emission factors.
  def prodDivide(): GHGData = {
    GHGData(
      total_area, production,
      emissions_co2_yearly,
      emissions_ch4_yearly,
      emissions_n2o_yearly,
      emissions_yearly,
      ef_co2_yearly.divide(production),
      ef_ch4_yearly.divide(production),
      ef_n2o_yearly.divide(production),
      emissions_factor_yearly.divide(production)
    )
  }

  def merge(other: GHGData): GHGData = {

    GHGData(
      total_area.merge(other.total_area),
      production.merge(other.production),

      emissions_co2_yearly.merge(other.emissions_co2_yearly),
      emissions_ch4_yearly.merge(other.emissions_ch4_yearly),
      emissions_n2o_yearly.merge(other.emissions_n2o_yearly),
      emissions_yearly.merge(other.emissions_yearly),

      ef_co2_yearly.merge(other.ef_co2_yearly),
      ef_ch4_yearly.merge(other.ef_ch4_yearly),
      ef_n2o_yearly.merge(other.ef_n2o_yearly),
      emissions_factor_yearly.merge(other.emissions_factor_yearly)
    )
  }
}

object GHGData {

  def empty: GHGData =
    GHGData(
      GHGDataDouble.empty,
      GHGDataDouble.empty,
      GHGDataValueYearly.empty,
      GHGDataValueYearly.empty,
      GHGDataValueYearly.empty,
      GHGDataValueYearly.empty,
      GHGDataValueYearly.empty,
      GHGDataValueYearly.empty,
      GHGDataValueYearly.empty,
      GHGDataValueYearly.empty
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
