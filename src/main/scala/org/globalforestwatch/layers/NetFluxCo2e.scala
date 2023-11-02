package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class NetFluxCo2e(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_full_extent_net_flux"

  val uri: String =
    uriForGrid(gridTile, kwargs)


//  // For carbonflux package run only
//  val datasetName = "Na"
//
//  val model_suffix: String = if (model == "standard") "standard" else s"$model"
//  val uri: String =
//    s"s3://gfw-files/flux_1_2_3/net_flux_all_forest_types_all_drivers/$model_suffix/${gridTile.tileId}.tif"
}
