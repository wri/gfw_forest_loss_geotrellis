package org.globalforestwatch.layers

class LandRights(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/land_rights/$grid.tif"
}
