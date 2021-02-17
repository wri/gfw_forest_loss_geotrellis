package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GFWProCoverage(gridTile: GridTile)
    extends MapLayer
    with OptionalILayer {

  // FIXME: verify source path
  val uri: String =
    s"$basePath/gfwpro_coverage/v1/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/coverage/gdal-geotiff/${gridTile.tileId}.tif"

  def lookup(value: Int): Map[String, Boolean] = {
    val bits = "0000000" + value.toBinaryString takeRight 8
    // FIXME: verify bit order
    Map(
      "South America" -> (bits(7) == '1'),
      "Legal Amazon" -> (bits(6) == '1'),
      "Brazil Biomes" -> (bits(5) == '1'),
      "Cerrado Biomes" -> (bits(4) == '1'),
      "South East Asia" -> (bits(3) == '1'),
      "Indonesia" -> (bits(2) == '1')
    )
  }
}
