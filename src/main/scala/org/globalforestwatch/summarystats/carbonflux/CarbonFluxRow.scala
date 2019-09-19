package org.globalforestwatch.summarystats.carbonflux

import org.globalforestwatch.features.GadmFeatureId

case class CarbonFluxRow(id: GadmFeatureId,
                         data_group: CarbonFluxDataGroup,
                         data: CarbonFluxData)
