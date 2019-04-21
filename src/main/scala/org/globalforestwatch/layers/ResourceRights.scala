package org.globalforestwatch.layers

class ResourceRights(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/resource_rights/$grid.tif"
}
