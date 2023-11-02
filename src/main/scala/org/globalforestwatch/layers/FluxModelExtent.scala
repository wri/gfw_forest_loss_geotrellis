package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class FluxModelExtent(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "Na"

  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  val uri: String =
      s"s3://gfw-files/flux_1_2_3/model_extent/$model_suffix/${gridTile.tileId}.tif"
}
