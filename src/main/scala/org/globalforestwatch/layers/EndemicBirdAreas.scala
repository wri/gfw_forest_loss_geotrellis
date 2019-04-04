package org.globalforestwatch.layers

class EndemicBirdAreas(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/endemic_bird_areas/${grid}.tif"
}
