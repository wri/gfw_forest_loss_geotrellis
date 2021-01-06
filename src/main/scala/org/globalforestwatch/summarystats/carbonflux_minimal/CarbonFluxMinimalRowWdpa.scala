package org.globalforestwatch.summarystats.carbonflux_minimal

import org.globalforestwatch.features.WdpaFeatureId

case class CarbonFluxMinimalRowWdpa(id: WdpaFeatureId,
                                    dataGroup: CarbonFluxMinimalDataGroup,
                                    data: CarbonFluxMinimalData)
