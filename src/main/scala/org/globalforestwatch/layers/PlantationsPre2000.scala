package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.layers.{BooleanLayer, OptionalILayer}

case class PlantationsPre2000(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "Na"

  val uri: String =
    s"s3://gfw2-data/climate/carbon_model/other_emissions_inputs/IDN_MYS_plantation_pre_2000/processed/20200724/${gridTile.tileId}_plantation_2000_or_earlier_processed.tif"
}