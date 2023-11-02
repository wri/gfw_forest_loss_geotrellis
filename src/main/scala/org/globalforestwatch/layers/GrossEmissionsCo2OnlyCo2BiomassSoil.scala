package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossEmissionsCo2OnlyCo2BiomassSoil(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_full_extent_co2_gross_emissions"

  val uri: String =
    uriForGrid(gridTile, kwargs)

  //  val datasetName = "Na"
  //
  //  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  //  val uri: String =
  //    s"s3://gfw-files/flux_1_2_3/gross_emissions_co2_only_co2e/$model_suffix/${gridTile.tileId}.tif"
}
