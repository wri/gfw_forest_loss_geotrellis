package org.globalforestwatch.summarystats.annualupdate_minimal

import com.monovore.decline.Opts
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import org.globalforestwatch.config.RasterCatalog
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats.annualupdate_minimal.AnnualUpdateMinimalAnalysis.getOutputUrl

object AnnualUpdateMinimalCommand extends SummaryCommand {
  val changeOnlyOpt: Opts[Boolean] =
    Opts.flag("change_only", "Process change only").orFalse

  val annualupdateMinimalCommand: Opts[Unit] = Opts.subcommand(
    name = AnnualUpdateMinimalAnalysis.name,
    help = "Compute summary statistics for GFW dashboards."
  ) {
    (defaultOptions, featureFilterOptions, changeOnlyOpt).mapN { (default, filterOptions, changeOnly) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "changeOnly" -> changeOnly
      )

      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      val summaryDF = runAnalysis { spark =>
        val featureRDD = FeatureRDD(default.featureUris, default.featureType, featureFilter, default.splitFeatures, spark)
        AnnualUpdateMinimalAnalysis(featureRDD, default.featureType, spark, kwargs)
      }

      val runOutputUrl: String = getOutputUrl(kwargs)

      AnnualUpdateMinimalExport.export(
        default.featureType,
        summaryDF,
        runOutputUrl,
        kwargs
      )
    }
  }
}
