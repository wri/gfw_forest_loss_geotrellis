package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class SoyPlantedAreas(gridTile: GridTile)
    extends BooleanLayer
      with OptionalILayer {
  val uri: String =
    s"$basePath/umd_soy_planted_area/v1/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is__year_2020/geotiff/${gridTile.tileId}.tif"
  //
  //  def lookup(value: Float): Map[String, Boolean] = {
  //    // Geotrellis interprets Uint32 pixels as Float values
  //    // Somehow they also come back as very low values in `E` notation.
  //    // Here we convert them into the correct Long value ie 9.18355E-41 -> 918355
  //
  //    val valueString: String = value.toString
  //    val valueLong: Long = valueString.split("E")(0).replace(".", "").toLong
  //
  //    if (valueLong > math.pow(2,20)) throw new RuntimeException(s"Value ${valueLong} out of Range")
  //
  //    val bits = "0" * 19 + valueLong.toBinaryString takeRight 20
  //    Map(
  //      "2001" -> (bits(19) == '1'),
  //      "2002" -> (bits(18) == '1'),
  //      "2003" -> (bits(17) == '1'),
  //      "2004" -> (bits(16) == '1'),
  //      "2005" -> (bits(15) == '1'),
  //      "2006" -> (bits(14) == '1'),
  //      "2007" -> (bits(13) == '1'),
  //      "2008" -> (bits(12) == '1'),
  //      "2009" -> (bits(11) == '1'),
  //      "2010" -> (bits(10) == '1'),
  //      "2011" -> (bits(9) == '1'),
  //      "2012" -> (bits(8) == '1'),
  //      "2013" -> (bits(7) == '1'),
  //      "2014" -> (bits(6) == '1'),
  //      "2015" -> (bits(5) == '1'),
  //      "2016" -> (bits(4) == '1'),
  //      "2017" -> (bits(3) == '1'),
  //      "2018" -> (bits(2) == '1'),
  //      "2019" -> (bits(1) == '1'),
  //      "2020" -> (bits(0) == '1')
  //    )
  //  }
}
