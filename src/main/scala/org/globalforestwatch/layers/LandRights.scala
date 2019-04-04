package org.globalforestwatch.layers

class LandRights(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/land_rights/${grid}.tif"
}
