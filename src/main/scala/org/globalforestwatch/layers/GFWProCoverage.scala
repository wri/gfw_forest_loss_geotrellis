package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GFWProCoverage(gridTile: GridTile)
    extends MapILayer
      with OptionalILayer {

  val uri: String =
    s"$basePath/gfwpro_forest_change_regions/v20210129/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/bit_encoding/gdal-geotiff/${gridTile.tileId}.tif"

  def lookup(value: Int): Map[String, Boolean] = {
    val bits = "0000000" + value.toBinaryString takeRight 8
    Map(
      "South America" -> (bits(6) == '1'),
      "Legal Amazon" -> (bits(5) == '1'),
      "Brazil Biomes" -> (bits(4) == '1'),
      "Cerrado Biomes" -> (bits(3) == '1'),
      "South East Asia" -> (bits(2) == '1'),
      "Indonesia" -> (bits(1) == '1')
    )
  }
}
