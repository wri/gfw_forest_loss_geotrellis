package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class RemovalForestType(gridTile: GridTile, model: String = "standard")
  extends StringLayer
    with OptionalILayer {
  //    val model_suffix = if (model == "standard") s"__standard" else s"__$model"
  val model_suffix = s"__$model"

  val uri: String =
    s"$basePath/gfw_removal_forest_type$model_suffix/v20150601/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/type/geotiff/${gridTile.tileId}.tif"

  def lookup(value: Int): String = value match {
    case 1 => "IPCC default old (>20 year) secondary and primary rates"
    case 2 => "Young (<20 year) natural forest rates"
    case 3 => "US-specific rates"
    case 4 => "Planted forest rates"
    case 5 => "European forest rates"
    case 6 => "Mangrove rates"
    case _ => ""
  }
}
