package org.globalforestwatch.summarystats.carbonflux

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  *
  */
case class CarbonFluxData(
                           var lossYear: scala.collection.mutable.Map[Int, CarbonFluxYearData],
                           var count: Int,
                           var extent2000: Double,
                           var totalArea: Double,
                           var totalBiomass: Double,
                           var avgBiomass: Double,
                           var totalGrossAnnualRemovalsCarbon: Double,
                           var avgGrossAnnualRemovalsCarbon: Double,
                           var totalGrossCumulRemovalsCarbon: Double,
                           var avgGrossCumulRemovalsCarbon: Double,
                           var totalNetFluxCo2: Double,
                           var avgNetFluxCo2: Double,
                           var totalAgcEmisYear: Double,
                           var avgAgcEmisYear: Double,
                           var totalBgcEmisYear: Double,
                           var avgBgcEmisYear: Double,
                           var totalDeadwoodCarbonEmisYear: Double,
                           var avgDeadwoodCarbonEmisYear: Double,
                           var totalLitterCarbonEmisYear: Double,
                           var avgLitterCarbonEmisYear: Double,
                           var totalSoilCarbonEmisYear: Double,
                           var avgSoilCarbonEmisYear: Double,
                           var totalCarbonEmisYear: Double,
                           var avgTotalCarbonEmisYear: Double,
                           var totalAgc2000: Double,
                           var avgAgc2000: Double,
                           var totalBgc2000: Double,
                           var avgBgc2000: Double,
                           var totalDeadwoodCarbon2000: Double,
                           var avgDeadwoodCarbon2000: Double,
                           var totalLitterCarbon2000: Double,
                           var avgLitterCarbon2000: Double,
                           var totalSoil2000Year: Double,
                           var avgSoilCarbon2000: Double,
                           var totalCarbon2000: Double,
                           var avgTotalCarbon2000: Double,
                           var totalGrossEmissionsCo2: Double,
                           var avgGrossEmissionsCo2: Double
) {
  def merge(other: CarbonFluxData): CarbonFluxData = {

    CarbonFluxData(
      lossYear ++ other.lossYear.map {
        case (k, v) => {
          val loss: CarbonFluxYearData = lossYear(k)
          var otherLoss: CarbonFluxYearData = v
          otherLoss.area_loss += loss.area_loss
          otherLoss.biomass_loss += loss.biomass_loss
          otherLoss.gross_emissions_co2 += loss.gross_emissions_co2
          k -> otherLoss
        }
      },
      count + other.count,
      extent2000 + other.extent2000,
      totalArea + other.totalArea,
      totalBiomass + other.totalBiomass,
      ((avgBiomass * count) + (other.avgBiomass * other.count)) / (count + other.count),
      totalGrossAnnualRemovalsCarbon + other.totalGrossAnnualRemovalsCarbon,
      ((avgGrossAnnualRemovalsCarbon * count) + (other.avgGrossAnnualRemovalsCarbon * other.count)) / (count + other.count),
      totalGrossCumulRemovalsCarbon + other.totalGrossCumulRemovalsCarbon,
      ((avgGrossCumulRemovalsCarbon * count) + (other.avgGrossCumulRemovalsCarbon * other.count)) / (count + other.count),
      totalNetFluxCo2 + other.totalNetFluxCo2,
      ((avgNetFluxCo2 * count) + (other.avgNetFluxCo2 * other.count)) / (count + other.count),
      totalAgcEmisYear + other.totalAgcEmisYear,
      ((avgAgcEmisYear * count) + (other.avgAgcEmisYear * other.count)) / (count + other.count),
      totalBgcEmisYear + other.totalBgcEmisYear,
      ((avgBgcEmisYear * count) + (other.avgBgcEmisYear * other.count)) / (count + other.count),
      totalDeadwoodCarbonEmisYear + other.totalDeadwoodCarbonEmisYear,
      ((avgDeadwoodCarbonEmisYear * count) + (other.avgDeadwoodCarbonEmisYear * other.count)) / (count + other.count),
      totalLitterCarbonEmisYear + other.totalLitterCarbonEmisYear,
      ((avgLitterCarbonEmisYear * count) + (other.avgLitterCarbonEmisYear * other.count)) / (count + other.count),
      totalSoilCarbonEmisYear + other.totalSoilCarbonEmisYear,
      ((avgSoilCarbonEmisYear * count) + (other.avgSoilCarbonEmisYear * other.count)) / (count + other.count),
      totalCarbonEmisYear + other.totalCarbonEmisYear,
      ((avgTotalCarbonEmisYear * count) + (other.avgTotalCarbonEmisYear * other.count)) / (count + other.count),
      totalAgc2000 + other.totalAgc2000,
      ((avgAgc2000 * count) + (other.avgAgc2000 * other.count)) / (count + other.count),
      totalBgc2000 + other.totalBgc2000,
      ((avgBgc2000 * count) + (other.avgBgc2000 * other.count)) / (count + other.count),
      totalDeadwoodCarbon2000 + other.totalDeadwoodCarbon2000,
      ((avgDeadwoodCarbon2000 * count) + (other.avgDeadwoodCarbon2000 * other.count)) / (count + other.count),
      totalLitterCarbon2000 + other.totalLitterCarbon2000,
      ((avgLitterCarbon2000 * count) + (other.avgLitterCarbon2000 * other.count)) / (count + other.count),
      totalSoil2000Year + other.totalSoil2000Year,
      ((avgSoilCarbon2000 * count) + (other.avgSoilCarbon2000 * other.count)) / (count + other.count),
      totalCarbon2000 + other.totalCarbon2000,
      ((avgTotalCarbon2000 * count) + (other.avgTotalCarbon2000 * other.count)) / (count + other.count),
      totalGrossEmissionsCo2 + other.totalGrossEmissionsCo2,
      ((avgGrossEmissionsCo2 * count) + (other.avgGrossEmissionsCo2 * other.count)) / (count + other.count)
    )
  }
}

object CarbonFluxData {
  implicit val lossDataSemigroup: Semigroup[CarbonFluxData] =
    new Semigroup[CarbonFluxData] {
      def combine(x: CarbonFluxData, y: CarbonFluxData): CarbonFluxData =
        x.merge(y)
    }

}
