package org.globalforestwatch.layers

class UrbanWatersheds(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/urb_watersheds/$grid.tif"
}
