package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{GeostoreFeatureId, FireAlertViirsFeatureId}

case class FireAlertsRowViirsGeostore(fireId: FireAlertViirsFeatureId,
                                      geostoreId: GeostoreFeatureId,
                                      dataGroup: FireAlertsDataGroup,
                                      data: FireAlertsData)
