package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class BurnYearHansenLoss(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntegerLayer
    with OptionalILayer {

  val datasetName = "Na"


  val uri: String =
      s"s3://gfw-files/flux_1_2_2/burn_year_with_Hansen_loss/${gridTile.tileId}.tif"

  override def lookup(value: Int): Integer =
    if (value == 0) null else value + 2000
}
