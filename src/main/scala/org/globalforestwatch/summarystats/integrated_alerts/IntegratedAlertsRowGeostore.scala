package org.globalforestwatch.summarystats.integrated_alerts

import org.globalforestwatch.features.{GeostoreFeatureId, SimpleFeatureId}

case class IntegratedAlertsRowGeostore(id: GeostoreFeatureId,
                               dataGroup: IntegratedAlertsDataGroup,
                               data: IntegratedAlertsData)
