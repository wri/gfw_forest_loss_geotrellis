package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.layers.{BooleanLayer, OptionalILayer}

case class PlantationsPre2000(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "Na"

  val uri: String =
    s"s3://gfw-data-lake/gfw_pre_2000_plantations/v20200724/raw/${gridTile.tileId}_plantation_2000_or_earlier_processed.tif"
}