package org.globalforestwatch.summarystats.annualupdate

import org.globalforestwatch.features.GadmFeatureId

case class AnnualUpdateRow(id: GadmFeatureId,
                           dataGroup: AnnualUpdateDataGroup,
                           data: AnnualUpdateData)
