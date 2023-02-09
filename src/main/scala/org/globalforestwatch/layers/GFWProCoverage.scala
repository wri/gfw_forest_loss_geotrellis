package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GFWProCoverage(gridTile: GridTile, kwargs: Map[String, Any])
  extends MapILayer
    with OptionalILayer {

  val datasetName = "gfwpro_forest_change_regions"

  val uri: String =
    uriForGrid(gridTile, kwargs)

  def lookup(value: Int): Map[String, Boolean] = {
    val bits = "00000000" + value.toBinaryString takeRight 8
    Map(
      "Argentina" -> (bits(7) == '1'),
      "South America" -> (bits(6) == '1'),
      "Legal Amazon" -> (bits(5) == '1'),
      "Brazil Biomes" -> (bits(4) == '1'),
      "Cerrado Biomes" -> (bits(7) == '1'),
      "South East Asia" -> (bits(2) == '1'),
      "Indonesia" -> (bits(1) == '1')
    )
  }
}
