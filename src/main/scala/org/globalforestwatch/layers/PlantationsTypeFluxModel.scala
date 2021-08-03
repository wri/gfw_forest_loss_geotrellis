package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class PlantationsTypeFluxModel(gridTile: GridTile)
  extends StringLayer
    with OptionalILayer {

  val uri: String =
//    s"$basePath/gfw_plantations_flux_model/v2.1/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/type/geotiff/${gridTile.tileId}.tif"
    s"s3://gfw-files/flux_1_2_0/plantation_type/standard/${gridTile.tileId}.tif"

  override val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {
    case 1  => "Oil palm"
    case 2  => "Wood fiber"
    case 3  => "Other"
    case _ => "Unknown"
  }
}
