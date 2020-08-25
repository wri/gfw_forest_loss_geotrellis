package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GainYearCount(gridTile: GridTile, model: String = "standard")
  extends IntegerLayer
    with OptionalILayer {
  //    val model_suffix = if (model == "standard") s"__standard" else s"__$model"
  val model_suffix = s"__$model"
  val uri: String =
    s"$basePath/gfw_gain_year_count/v20160111$model_suffix/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/year/geotiff/${gridTile.tileId}.tif"

  override def lookup(value: Int): Integer = if (value == 0) null else value
}
