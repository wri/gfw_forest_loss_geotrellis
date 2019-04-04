package org.globalforestwatch.layers

class ResourceRights(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/resource_rights/${grid}.tif"
}
