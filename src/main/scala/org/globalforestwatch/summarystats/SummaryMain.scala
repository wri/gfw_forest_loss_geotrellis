package org.globalforestwatch.summarystats

import org.globalforestwatch.summarystats.annualupdate_minimal.AnnualUpdateMinimalCommand.annualupdateMinimalCommand
import org.globalforestwatch.summarystats.carbon_sensitivity.CarbonSensitivityCommand.carbonSensitivityCommand
import org.globalforestwatch.summarystats.carbonflux.CarbonFluxCommand.carbonFluxCommand
import org.globalforestwatch.summarystats.firealerts.FireAlertsCommand.fireAlertsCommand
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticCommand.forestChangeDiagnosticCommand
import org.globalforestwatch.summarystats.gfwpro_dashboard.GfwProDashboardCommand.gfwProDashboardCommand
import org.globalforestwatch.summarystats.gladalerts.GladAlertsCommand.gladAlertsCommand
import org.globalforestwatch.summarystats.treecoverloss.TreeCoverLossCommand.treeCoverLossCommand
import org.globalforestwatch.summarystats.integrated_alerts.IntegratedAlertsCommand.integratedAlertsCommand
import org.globalforestwatch.summarystats.afi.AFiCommand.afiCommand
import com.monovore.decline._

object SummaryMain {
  val name = "geotrellis-summary-stats"
  val header = "Compute summary statistics for GFW data"
  val main = {
    annualupdateMinimalCommand orElse
      carbonSensitivityCommand orElse
      carbonFluxCommand orElse
      fireAlertsCommand orElse
      forestChangeDiagnosticCommand orElse
      gfwProDashboardCommand orElse
      gladAlertsCommand orElse
      treeCoverLossCommand orElse
      integratedAlertsCommand orElse
      afiCommand
  }
  val command = Command(name, header, true)(main)

  final def main(args: Array[String]): Unit = {
    // Print out environment variables (for debugging purposes)
    val environmentVars = System.getenv().forEach {
      case (key, value) => println(s"$key = $value")
    }
    
    command.parse(args, sys.env) match {
      case Left(help) =>
        System.err.println(help)
        System.exit(2)
      case Right(_) => ()
    }
  }
}
