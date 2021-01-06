package org.globalforestwatch.summarystats.carbonflux_minimal

import org.globalforestwatch.features.SimpleFeatureId

case class CarbonFluxMinimalRowSimple(id: SimpleFeatureId,
                                      dataGroup: CarbonFluxMinimalDataGroup,
                                      data: CarbonFluxMinimalData)
