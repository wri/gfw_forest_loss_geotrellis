package org.globalforestwatch.carbonflux

import cats.Semigroup
import geotrellis.raster.histogram.StreamingHistogram

/** Summary data per class
  *
  * Note: This case class contains mutable values
  *
  */
case class CarbonFluxData(
  var lossYear: scala.collection.mutable.Map[Int, CarbonFluxYearData],
  var extent2000: Double,
  var totalArea: Double,
  var totalBiomass: Double,
  var biomassHistogram: StreamingHistogram,
  var totalGrossAnnualRemovalsCarbon: Double,
  var grossAnnualRemovalsCarbonHistogram: StreamingHistogram,
  var totalGrossCumulRemovalsCarbon: Double,
  var grossCumulRemovalsCarbonHistogram: StreamingHistogram,
  var totalNetFluxCo2: Double,
  var netFluxCo2Histogram: StreamingHistogram,
  var totalAgcEmisYear: Double,
  var agcEmisYearHistogram: StreamingHistogram,
  var totalBgcEmisYear: Double,
  var bgcEmisYearHistogram: StreamingHistogram,
  var totalDeadwoodCarbonEmisYear: Double,
  var deadwoodCarbonEmisYearHistogram: StreamingHistogram,
  var totalLitterCarbonEmisYear: Double,
  var litterCarbonEmisYearHistogram: StreamingHistogram,
  var totalSoilCarbonEmisYear: Double,
  var soilCarbonEmisYearHistogram: StreamingHistogram,
  var totalCarbonEmisYear: Double,
  var totalCarbonEmisYearHistogram: StreamingHistogram,
  var totalAgc2000: Double,
  var agc2000Histogram: StreamingHistogram,
  var totalBgc2000: Double,
  var bgc2000Histogram: StreamingHistogram,
  var totalDeadwoodCarbon2000: Double,
  var deadwoodCarbon2000Histogram: StreamingHistogram,
  var totalLitterCarbon2000: Double,
  var litterCarbon2000Histogram: StreamingHistogram,
  var totalSoil2000Year: Double,
  var soilCarbon2000Histogram: StreamingHistogram,
  var totalCarbon2000: Double,
  var totalCarbon2000Histogram: StreamingHistogram,
  var totalGrossEmissionsCo2: Double,
  var grossEmissionsCo2Histogram: StreamingHistogram
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
      extent2000 + other.extent2000,
      totalArea + other.totalArea,
      totalBiomass + other.totalBiomass,
      biomassHistogram.merge(other.biomassHistogram),
      totalGrossAnnualRemovalsCarbon + other.totalGrossAnnualRemovalsCarbon,
      grossAnnualRemovalsCarbonHistogram.merge(
        other.grossAnnualRemovalsCarbonHistogram
      ),
      totalGrossCumulRemovalsCarbon + other.totalGrossCumulRemovalsCarbon,
      grossCumulRemovalsCarbonHistogram.merge(
        other.grossCumulRemovalsCarbonHistogram
      ),
      totalNetFluxCo2 + other.totalNetFluxCo2,
      netFluxCo2Histogram.merge(other.netFluxCo2Histogram),
      totalAgcEmisYear + other.totalAgcEmisYear,
      agcEmisYearHistogram.merge(other.agcEmisYearHistogram),
      totalBgcEmisYear + other.totalBgcEmisYear,
      bgcEmisYearHistogram.merge(other.bgcEmisYearHistogram),
      totalDeadwoodCarbonEmisYear + other.totalDeadwoodCarbonEmisYear,
      deadwoodCarbonEmisYearHistogram.merge(deadwoodCarbonEmisYearHistogram),
      totalLitterCarbonEmisYear + other.totalLitterCarbonEmisYear,
      litterCarbonEmisYearHistogram.merge(other.litterCarbonEmisYearHistogram),
      totalSoilCarbonEmisYear + other.totalSoilCarbonEmisYear,
      soilCarbonEmisYearHistogram.merge(other.soilCarbonEmisYearHistogram),
      totalCarbonEmisYear + other.totalCarbonEmisYear,
      totalCarbonEmisYearHistogram.merge(other.totalCarbonEmisYearHistogram),
      totalAgc2000 + other.totalAgc2000,
      agc2000Histogram.merge(other.agc2000Histogram),
      totalBgc2000 + other.totalBgc2000,
      bgc2000Histogram.merge(other.bgc2000Histogram),
      totalDeadwoodCarbon2000 + other.totalDeadwoodCarbon2000,
      deadwoodCarbon2000Histogram.merge(other.deadwoodCarbon2000Histogram),
      totalLitterCarbon2000 + other.totalLitterCarbon2000,
      litterCarbon2000Histogram.merge(other.litterCarbon2000Histogram),
      totalSoil2000Year + other.totalSoil2000Year,
      soilCarbon2000Histogram.merge(other.soilCarbon2000Histogram),
      totalCarbon2000 + other.totalCarbon2000,
      totalCarbon2000Histogram.merge(other.totalCarbon2000Histogram),
      totalGrossEmissionsCo2 + other.totalGrossEmissionsCo2,
      grossEmissionsCo2Histogram.merge(other.grossEmissionsCo2Histogram)
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
