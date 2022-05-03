package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class StdevAnnualAbovegroundRemovalsCarbon(gridTile: GridTile,
                                                model: String = "standard", kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "Na"


  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  val uri: String =
      s"s3://gfw-files/flux_1_2_2/stdev_annual_removal_factor_AGC_all_forest_types/$model_suffix/${gridTile.tileId}.tif"
}
