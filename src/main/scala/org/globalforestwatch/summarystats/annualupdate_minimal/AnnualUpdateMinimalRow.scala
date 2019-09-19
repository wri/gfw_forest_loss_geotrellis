package org.globalforestwatch.summarystats.annualupdate_minimal

import org.globalforestwatch.features.GadmFeatureId

case class AnnualUpdateMinimalRow(id: GadmFeatureId,
                                  dataGroup: AnnualUpdateMinimalDataGroup,
                                  data: AnnualUpdateMinimalData)
