package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class AgcEmisYear(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "Na"


  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  val uri: String =
      s"s3://gfw-files/flux_1_2_2/agc_emis_year/$model_suffix/${gridTile.tileId}.tif"
}
