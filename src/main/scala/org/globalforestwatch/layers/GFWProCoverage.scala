package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GFWProCoverage(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntegerLayer
    with OptionalILayer {

  val datasetName = "gfwpro_forest_change_regions"

  val uri: String =
    uriForGrid(gridTile, kwargs)
}

object GFWProCoverage {
  def isArgentina(v: Integer): Boolean = (v & (1 << 0)) != 0
  def isColombia(v: Integer): Boolean  = (v & (1 << 1)) != 0
  def isSouthAmerica(v: Integer): Boolean  = (v & (1 << 2)) != 0
  def isLegalAmazonPresence(v: Integer): Boolean  = (v & (1 << 3)) != 0
  // Brazil Biomes presence is true for all Brazil, except for a few narrow costal
  // areas.
  def isBrazilBiomesPresence(v: Integer): Boolean  = (v & (1 << 4)) != 0
  def isCerradoBiomesPresence(v: Integer): Boolean  = (v & (1 << 5)) != 0
  def isSouthEastAsia(v: Integer): Boolean  = (v & (1 << 6)) != 0
  def isIndonesia(v: Integer): Boolean  = (v & (1 << 7)) != 0
}
