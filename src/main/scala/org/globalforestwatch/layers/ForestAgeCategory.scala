package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ForestAgeCategory(gridTile: GridTile, model: String = "standard")
  extends StringLayer
    with OptionalILayer {
  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  val uri: String =
//    s"$basePath/gfw_forest_age_category/v20191106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/category/gdal-geotiff/${gridTile.tileId}.tif"
    s"s3://gfw-files/flux_1_2_1/forest_age_category/$model_suffix/${gridTile.tileId}.tif"

  override val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {
    case 1  => "Secondary forest <=20 years"
    case 2  => "Secondary forest >20 years"
    case 3  => "Primary forest"
    case _ => "Unknown"
  }
}
