package org.globalforestwatch.summarystats.annualupdate_minimal

import org.globalforestwatch.features.GadmFeatureId

case class AnnualUpdateMinimalRowGadm(id: GadmFeatureId,
                                      dataGroup: AnnualUpdateMinimalDataGroup,
                                      data: AnnualUpdateMinimalData)
