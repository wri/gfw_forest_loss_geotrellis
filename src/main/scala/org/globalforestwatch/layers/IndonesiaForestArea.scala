package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class IndonesiaForestArea(gridTile: GridTile)
    extends StringLayer
    with OptionalILayer {

  val uri: String = s"$basePath/idn_forest_area/v201909/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/type/geotiff/${gridTile.tileId}.tif"

  override val externalNoDataValue: String = ""

  def lookup(value: Int): String = value match {
    case 1001        => "Protected Forest"
    case 1003        => "Production Forest"
    case 1004        => "Limited Production Forest"
    case 1005        => "Converted Production Forest"
    case 1007        => "Other Utilization Area"
    case 5001 | 5003 => ""
    case 1 | 1002 | 10021 | 10022 | 10023 | 10024 | 10025 | 10026 =>
      "Sanctuary Reserves/Nature Conservation Area"
    case 100201 | 100211 | 100221 | 100241 | 100251 => "Marine Protected Areas"
    case _ => ""

  }
}
