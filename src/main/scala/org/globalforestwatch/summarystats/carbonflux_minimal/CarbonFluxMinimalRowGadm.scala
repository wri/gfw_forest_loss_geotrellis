package org.globalforestwatch.summarystats.carbonflux_minimal

import org.globalforestwatch.features.GadmFeatureId

case class CarbonFluxMinimalRowGadm(id: GadmFeatureId,
                                    dataGroup: CarbonFluxMinimalDataGroup,
                                    data: CarbonFluxMinimalData)
