package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class BurnYearHansenLoss(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntegerLayer
    with OptionalILayer {

  val datasetName = "Na"
  override lazy val version = "Na"

  val uri: String =
  //    s"$basePath/gfw_burn_year_Hansen_loss/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/year/geotiff/${gridTile.tileId}.tif"
    s"s3://gfw-files/flux_1_2_1/burn_year_with_Hansen_loss/${gridTile.tileId}.tif"

  override def lookup(value: Int): Integer =
    if (value == 0) null else value + 2000
}
