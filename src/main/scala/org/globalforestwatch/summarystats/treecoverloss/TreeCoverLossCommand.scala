package org.globalforestwatch.summarystats.treecoverloss

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.config.GfwConfig
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
      "Contextual layers to filter by (currently supported: is__umd_regional_primary_forest_2001, is__gfw_plantations, is__global_peat, " +
        "is__tree_cover_loss_from_fires, is__umd_tree_cover_loss, is__intact_forest_landscape_2000"
    )
    .withDefault(NonEmptyList.of(""))

  val carbonPoolOpts: Opts[Boolean] = Opts
    .flag(
      "carbon_pools",
      "Carbon pools to optionally include. Currently can include: gfw_aboveground_carbon_stock_2000__Mg, gfw_belowground_carbon_stock_2000__Mg, gfw_soil_carbon_stock_2000__Mg"
    )
    .orFalse

  val simpleAGBEmisOpts: Opts[Boolean] = Opts
    .flag(
      "simple_agb_emissions",
      "Calculate emissions from tree cover loss in AGB (simple emissions model) following Zarin et al. 2016"
    )
    .orFalse

  val emisGasAnnualOpts: Opts[Boolean] = Opts
    .flag(
      "emissions_by_gas_annually",
      "Output emissions for CO2 and non-CO2 gases (CH4 and N2O) separately"
    )
    .orFalse

  val treeCoverLossOptions: Opts[(NonEmptyList[String], Int, Product with Serializable, Boolean, Boolean, Boolean)] = // If new options are added below, the corresponding types must be added here
    (contextualLayersOpts, tcdOpt, thresholdOpts, carbonPoolOpts, simpleAGBEmisOpts, emisGasAnnualOpts).tupled   // If new options are added here, the corresponding types must be added in the row above

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
        "carbonPools" -> treeCoverLoss._4,
        "simpleAGBEmis" -> treeCoverLoss._5,
        "emisGasAnnual" -> treeCoverLoss._6,
        "config" -> GfwConfig.get()
      )
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { spark =>
        val featureRDD = FeatureRDD(default.featureUris, default.featureType, featureFilter, default.splitFeatures, spark)
        TreeLossAnalysis(featureRDD, default.featureType, spark, kwargs)
      }
    }
  }
}
