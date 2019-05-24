package org.globalforestwatch.layers

case class EndemicBirdAreas(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/endemic_bird_areas/$grid.tif"
}
