package org.globalforestwatch.layers

case class Aze(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/birdlife_alliance_for_zero_extinction_site/v20190816/raster/epsg-4326/$grid.tif"
}
