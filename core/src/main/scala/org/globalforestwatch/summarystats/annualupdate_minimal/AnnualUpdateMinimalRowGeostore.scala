package org.globalforestwatch.summarystats.annualupdate_minimal

import org.globalforestwatch.features.GeostoreFeatureId

case class AnnualUpdateMinimalRowGeostore(id: GeostoreFeatureId,
                                      dataGroup: AnnualUpdateMinimalDataGroup,
                                      data: AnnualUpdateMinimalData)