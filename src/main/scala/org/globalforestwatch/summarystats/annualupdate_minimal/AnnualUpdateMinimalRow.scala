package org.globalforestwatch.summarystats.annualupdate_minimal

import org.globalforestwatch.features.GadmFeatureId

case class AnnualUpdateMinimalRow(lossRowFeatureId: GadmFeatureId,
                                  //                                  threshold: Integer,
                                  layers: AnnualUpdateMinimalDataGroup,
                                  extent2000: Double,
                                  extent2010: Double,
                                  totalArea: Double,
                                  totalGainArea: Double,
                                  totalBiomass: Double,
                                  totalCo2: Double,
                                  meanBiomass: Option[Double],
                                  //                       totalMangroveBiomass: Double,
                                  //                       totalMangroveCo2: Double,
                                  //                       meanMangroveBiomass: Option[Double],
                                  yearData: List[AnnualUpdateMinimalYearData])
