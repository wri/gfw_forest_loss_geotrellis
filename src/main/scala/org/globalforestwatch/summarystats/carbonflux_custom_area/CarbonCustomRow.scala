package org.globalforestwatch.summarystats.carbonflux_custom_area

import org.globalforestwatch.features.GadmFeatureId

case class CarbonCustomRow(id: GadmFeatureId,
                           dataGroup: CarbonCustomDataGroup,
                           data: CarbonCustomData)
