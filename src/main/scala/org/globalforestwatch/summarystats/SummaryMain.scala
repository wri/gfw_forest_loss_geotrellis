package org.globalforestwatch.summarystats

import com.monovore.decline.CommandApp
import org.globalforestwatch.summarystats.annualupdate_minimal.AnnualUpdateMinimalCommand.annualupdateMinimalCommand
import org.globalforestwatch.summarystats.carbon_sensitivity.CarbonSensitivityCommand.carbonSensitivityCommand
import org.globalforestwatch.summarystats.carbonflux.CarbonFluxCommand.carbonFluxCommand
import org.globalforestwatch.summarystats.firealerts.FireAlertsCommand.fireAlertsCommand
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticCommand.forestChangeDiagnosticCommand
import org.globalforestwatch.summarystats.gfwpro_dashboard.GfwProDashboardCommand.gfwProDashboardCommand
import org.globalforestwatch.summarystats.gladalerts.GladAlertsCommand.gladAlertsCommand
import org.globalforestwatch.summarystats.treecoverloss.TreeCoverLossCommand.treeCoverLossCommand

object SummaryMain
  extends CommandApp(
    name = "geotrellis-summary-stats",
    header = "Compute summary statistics for GFW data",
    main = {
      annualupdateMinimalCommand orElse
        carbonSensitivityCommand orElse
        carbonFluxCommand orElse
        fireAlertsCommand orElse
        forestChangeDiagnosticCommand orElse
        gfwProDashboardCommand orElse
        gladAlertsCommand orElse
        treeCoverLossCommand

    }
  )
