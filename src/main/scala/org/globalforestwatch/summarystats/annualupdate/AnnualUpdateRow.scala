package org.globalforestwatch.summarystats.annualupdate

import org.globalforestwatch.features.GadmFeatureId

case class AnnualUpdateRow(lossRowFeatureId: GadmFeatureId,
//                           threshold: Integer,
                           layers: AnnualUpdateDataGroup,
                           extent2000: Double,
                           extent2010: Double,
                           totalArea: Double,
                           totalGainArea: Double,
                           totalBiomass: Double,
                           totalCo2: Double,
                           meanBiomass: Option[Double],
                           totalMangroveBiomass: Double,
                           totalMangroveCo2: Double,
                           meanMangroveBiomass: Option[Double],
                           yearData: List[AnnualUpdateYearData])
