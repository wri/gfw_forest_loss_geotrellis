package org.globalforestwatch.summarystats.treecoverloss

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts

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

  val carbonPoolOpts: Opts[String] = Opts
    .option[String](
      "carbon_pools",
      "Carbon pools to optionally include. Currently can include: gfw_aboveground_carbon_stock_2000__Mg, gfw_belowground_carbon_stock_2000__Mg, gfw_soil_carbon_stock_2000__Mg"
    )
    .withDefault("exclude")


  val treeCoverLossOptions
  : Opts[(NonEmptyList[String], Int, Product with Serializable, String)] =
    (contextualLayersOpts, tcdOpt, thresholdOpts, carbonPoolOpts).tupled

  val treeCoverLossCommand: Opts[Unit] = Opts.subcommand(
    name = "treecoverloss",
    help = "Compute Tree Cover Loss Statistics."
  ) {
    (
      defaultOptions,
      treeCoverLossOptions,
      defaultFilterOptions,
      featureFilterOptions
      ).mapN { (default, treeCoverLoss, defaultFilter, featureFilter) =>
      val kwargs = Map(
        "outputUrl" -> default._3,
        "splitFeatures" -> default._4,
        "contextualLayers" -> treeCoverLoss._1,
        "tcdYear" -> treeCoverLoss._2,
        "thresholdFilter" -> treeCoverLoss._3,
        "carbonPools" -> treeCoverLoss._4,
        "idStart" -> featureFilter._1,
        "idEnd" -> featureFilter._2,
        "limit" -> defaultFilter._1,
        "tcl" -> defaultFilter._2,
        "glad" -> defaultFilter._3
      )

      runAnalysis("treecoverloss", default._1, default._2, kwargs)

    }
  }
}
