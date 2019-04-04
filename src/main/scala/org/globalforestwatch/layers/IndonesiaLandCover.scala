package org.globalforestwatch.layers

class IndonesiaLandCover(grid: String) extends StringLayer with OptionalILayer {

  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/idn_land_cover/${grid}.tif"

  def lookup(value: Int): String = value match {

    case 2001  => "Primary Dry Land Forest"
    case 2002  => "Secondary Dry Land Forest"
    case 2004  => "Primary Mangrove Forest"
    case 2005  => "Primary Swamp Forest"
    case 2006  => "Plantation Forest"
    case 2007  => "Bush / Shrub"
    case 2008  => null
    case 2010  => "Estate Crop Plantation"
    case 2011  => null
    case 2012  => "Settlement Area"
    case 2014  => "Bare Land"
    case 2020  => null
    case 2092  => null
    case 3000  => "Savannah"
    case 20021 => null
    case 20041 => "Secondary Mangrove Forest"
    case 20051 => "Secondary Swamp Forest"
    case 20071 => "Swamp Shrub"
    case 20091 => "Dryland Agriculture"
    case 20092 => "Shrub-Mixed Dryland Farm"
    case 20093 => "Rice Field"
    case 20094 => "Fish Pond"
    case 20102 => null
    case 20121 => "Airport	/ Harbour"
    case 20122 => "Transmigration Area"
    case 20141 => "Mining Area"
    case 20191 => null
    case 5001  => "Bodies of Water"
    case 50011 => "Swamp"
  }
}
