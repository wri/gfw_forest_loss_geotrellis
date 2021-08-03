package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class RSPO(gridTile: GridTile) extends StringLayer with OptionalILayer {

  val uri: String = s"$basePath/rspo_oil_palm/v20200114/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/certification_status/gdal-geotiff/${gridTile.tileId}.tif"

  def lookup(value: Int): String = value match {
    case 1 => "Certified"
    case 2 => "Unknown"
    case 3 => "Not certified"
    case _ => ""
  }
}
