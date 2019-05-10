package org.globalforestwatch.layers

case class ResourceRights(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/resource_rights/$grid.tif"
}
