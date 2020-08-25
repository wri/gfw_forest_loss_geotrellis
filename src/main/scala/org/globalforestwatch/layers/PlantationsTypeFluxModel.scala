package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class PlantationsTypeFluxModel(gridTile: GridTile)
  extends StringLayer
    with OptionalILayer {

  val uri: String =
    s"$basePath/gfw_plantations_flux_model/v1.3/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/type/geotiff/${gridTile.tileId}.tif"

  def lookup(value: Int): String = value match {
    case 1  => "Oil palm"
    case 2  => "Woof fiber"
    case 3  => "Other"
    case _ => ""
  }
}
