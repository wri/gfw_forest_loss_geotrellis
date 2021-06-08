package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts

object ForestChangeDiagnosticCommand extends SummaryCommand {


  val intermediateListSourceOpt: Opts[Option[NonEmptyList[String]]] = Opts
    .options[String](
      "intermediate_list_source",
      help = "URI of intermediate list results in TSV format"
    ).orNone

  val forestChangeDiagnosticCommand: Opts[Unit] = Opts.subcommand(
    name = "forest_change_diagnostic",
    help = "Compute summary statistics for GFW Pro Forest Change Diagnostic."
  ) {
    (
      defaultOptions,
      intermediateListSourceOpt,
      fireAlertOptions,
      defaultFilterOptions,
      featureFilterOptions,
      ).mapN { (default, intermediateListSource, fireAlert, defaultFilter, featureFilter) =>
      val kwargs = Map(
        "featureUris" -> default._2,
        "outputUrl" -> default._3,
        "splitFeatures" -> default._4,
        "intermediateListSource" -> intermediateListSource,
        "fireAlertType" -> fireAlert._1,
        "fireAlertSource" -> fireAlert._2,
        "idStart" -> featureFilter._1,
        "idEnd" -> featureFilter._2,
        "limit" -> defaultFilter._1,
        "tcl" -> defaultFilter._2,
        "glad" -> defaultFilter._3
      )

      runAnalysis("forest_change_diagnostic", default._1, default._2, kwargs)

    }
  }
}
