package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ForestAgeCategory(gridTile: GridTile, model: String = "standard")
  extends StringLayer
    with OptionalILayer {
  //    val model_suffix = if (model == "standard") s"__standard" else s"__$model"
  val model_suffix = s"__$model"
  val uri: String =
    s"$basePath/gfw_forest_age_category$model_suffix/v20191106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/category/geotiff/${gridTile.tileId}.tif"

  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 0  => "No age assigned"
    case 1  => "Secondary forest <=20 years"
    case 2  => "Secondary forest >20 years"
    case 3  => "Primary forest"
    case _ => "No age assigned"
  }
}
