package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.config.GfwConfig

case class IndonesiaLandCover(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "idn_land_cover_2017"
  val uri: String =
    uriForGrid(gridTile, kwargs)

  override val externalNoDataValue: String = ""
  private val fLookup = if (GfwConfig.isGfwPro) IndonesiaLandCover.proLabelTable else IndonesiaLandCover.flagshipLabelTable
  def lookup(value: Int): String = fLookup(value)
}

object IndonesiaLandCover {
  val flagshipLabelTable: Int => String = {
    case 2001  => "Primary Dry Land Forest"
    case 2002  => "Secondary Dry Land Forest"
    case 2004  => "Primary Mangrove Forest"
    case 2005  => "Primary Swamp Forest"
    case 2006  => "Plantation Forest"
    case 2007  => "Bush / Shrub"
    case 2008 => ""
    case 2010 => "Estate Crop Plantation"
    case 2011 => ""
    case 2012 => "Settlement Area"
    case 2014 => "Bare Land"
    case 2020 => ""
    case 2092 => ""
    case 3000 => "Savannah"
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

  val proLabelTable: Int => String = {
    case 2001  => "Primary forest"
    case 2002  => "Secondary forest"
    case 2004  => "Primary forest"
    case 2005  => "Primary forest"
    case 2006  => "Timber plantation"
    case 2007  => "Grassland/shrub"
    case 2008 => ""
    case 2010  => "Estate crop plantation"
    case 2011 => ""
    case 2012  => "Settlement"
    case 2014  => "Bare land"
    case 2020 => ""
    case 2092 => ""
    case 3000  => "Grassland/shrub"
    case 20021 => ""
    case 20041 => "Secondary forest"
    case 20051 => "Secondary forest"
    case 20071 => "Swamp"
    case 20091 => "Agriculture"
    case 20092 => "Agriculture"
    case 20093 => "Agriculture"
    case 20094 => "Fish pond"
    case 20102 => ""
    case 20121 => "Settlement"
    case 20122 => "Settlement"
    case 20141 => "Mining"
    case 20191 => ""
    case 5001  => "Body of water"
    case 50011 => "Swamp"
    case _ => ""
  }
}
