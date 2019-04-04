package org.globalforestwatch.layers

class UrbanWatersheds(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/urb_watersheds/${grid}.tif"
}
