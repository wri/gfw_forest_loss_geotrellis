package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class IndonesiaLandCover(gridTile: GridTile) extends StringLayer with OptionalILayer {

  val uri: String = s"$basePath/idn_land_cover_2017/v20180720/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/class/geotiff/${gridTile.tileId}.tif"

  override val externalNoDataValue: String = ""

  def lookup(value: Int): String = value match {

    case 2001  => "Primary Dry Land Forest"
    case 2002  => "Secondary Dry Land Forest"
    case 2004  => "Primary Mangrove Forest"
    case 2005  => "Primary Swamp Forest"
    case 2006  => "Plantation Forest"
    case 2007  => "Bush / Shrub"
    case 2008 => ""
    case 2010  => "Estate Crop Plantation"
    case 2011 => ""
    case 2012  => "Settlement Area"
    case 2014  => "Bare Land"
    case 2020 => ""
    case 2092 => ""
    case 3000  => "Savannah"
    case 20021 => ""
    case 20041 => "Secondary Mangrove Forest"
    case 20051 => "Secondary Swamp Forest"
    case 20071 => "Swamp Shrub"
    case 20091 => "Dryland Agriculture"
    case 20092 => "Shrub-Mixed Dryland Farm"
    case 20093 => "Rice Field"
    case 20094 => "Fish Pond"
    case 20102 => ""
    case 20121 => "Airport	/ Harbour"
    case 20122 => "Transmigration Area"
    case 20141 => "Mining Area"
    case 20191 => ""
    case 5001  => "Bodies of Water"
    case 50011 => "Swamp"
    case _ => ""
  }
}
