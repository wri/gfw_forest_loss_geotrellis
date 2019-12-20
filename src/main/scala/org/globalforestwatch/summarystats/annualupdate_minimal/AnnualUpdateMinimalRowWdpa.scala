package org.globalforestwatch.summarystats.annualupdate_minimal

import org.globalforestwatch.features.WdpaFeatureId

case class AnnualUpdateMinimalRowWdpa(id: WdpaFeatureId,
                                      dataGroup: AnnualUpdateMinimalDataGroup,
                                      data: AnnualUpdateMinimalData)
