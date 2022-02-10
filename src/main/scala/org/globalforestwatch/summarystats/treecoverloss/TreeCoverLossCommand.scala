package org.globalforestwatch.summarystats.treecoverloss

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.features._

object TreeCoverLossCommand extends SummaryCommand {

  val tcdOpt: Opts[Int] =
    Opts
      .option[Int]("tcd", help = "Select tree cover density year")
      .withDefault(2000)

  val thresholdOpts: Opts[Product with Serializable] = Opts
    .options[Int]("threshold", "Treecover threshold to apply")
    .withDefault(List(30))

  val contextualLayersOpts: Opts[NonEmptyList[String]] = Opts
    .options[String](
      "contextual_layer",
      "Contextual Layer to include (currently supported: is__umd_regional_primary_forest_2001, is__gfw_plantations"
    )
    .withDefault(NonEmptyList.of(""))

  val carbonPoolOpts: Opts[Boolean] = Opts
    .flag(
      "carbon_pools",
      "Carbon pools to optionally include. Currently can include: gfw_aboveground_carbon_stock_2000__Mg, gfw_belowground_carbon_stock_2000__Mg, gfw_soil_carbon_stock_2000__Mg"
    )
    .orFalse

  val treeCoverLossOptions: Opts[(NonEmptyList[String], Int, Product with Serializable, Boolean)] =
    (contextualLayersOpts, tcdOpt, thresholdOpts, carbonPoolOpts).tupled

  val treeCoverLossCommand: Opts[Unit] = Opts.subcommand(
    name = TreeLossAnalysis.name,
    help = "Compute Tree Cover Loss Statistics."
  ) {
    (
      defaultOptions,
      treeCoverLossOptions,
      featureFilterOptions
    ).mapN { (default, treeCoverLoss, filterOptions) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "contextualLayers" -> treeCoverLoss._1,
        "tcdYear" -> treeCoverLoss._2,
        "thresholdFilter" -> treeCoverLoss._3,
        "carbonPools" -> treeCoverLoss._4
      )
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { spark =>
        val featureRDD = FeatureRDD(default.featureUris, default.featureType, featureFilter, default.splitFeatures, spark)
        TreeLossAnalysis(featureRDD, default.featureType, spark, kwargs)
      }
    }
  }
}
