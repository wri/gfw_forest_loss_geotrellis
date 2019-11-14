package org.globalforestwatch.summarystats.carbon_sensitivity

import org.globalforestwatch.features.GadmFeatureId

case class CarbonSensitivityRow(id: GadmFeatureId,
                         data_group: CarbonSensitivityDataGroup,
                         data: CarbonSensitivityData)
