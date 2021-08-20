package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PeruForestConcessions(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "per_forest_concessions"
  val uri: String =
    s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/type/gdal-geotiff/${gridTile.tileId}.tif"


  override val externalNoDataValue: String = ""

  def lookup(value: Int): String = value match {
    case 1 => "Conservation"
    case 2 => "Ecotourism"
    case 3 => "Nontimber Forest Poducts (Nuts)"
    case 4 => "Nontimber Forest Poducts (Shiringa)"
    case 5 => "Reforestation"
    case 6 => "Timber Concession"
    case 7 => "Wildlife"
    case _ => ""
  }
}
