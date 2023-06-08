package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverLossLegalAmazon(gridTile: GridTile, kwargs: Map[String, Any]) extends IntegerLayer with OptionalILayer {

  val datasetName = "Na"

  val uri: String =
    s"s3://gfw-files/flux_1_2_0/legal_Amazon_annual_loss/${gridTile.tileId}.tif"

  override def lookup(value: Int): Integer = if (value == 0) null else value + 2000
}
