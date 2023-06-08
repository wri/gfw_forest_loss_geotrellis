package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class FiaRegionsUsExtent(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "Na"

  val uri: String =
    s"s3://gfw-files/flux_1_2_0/FIA_regions/${gridTile.tileId}.tif"

  override val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {
    case 1 => "PNWE"
    case 2 => "PNWW"
    case 3  => "PSW"
    case 4  => "RMS"
    case 5  => "NE"
    case 6  => "SE"
    case 7  => "NLS"
    case 8  => "SC"
    case 9  => "CS"
    case 10 => "GP"
    case 11 => "RMN"
    case _ => "Unknown"
  }
}