package org.globalforestwatch.layers

case class KeyBiodiversityAreas(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"$basePath/kba/v20191211/$grid.tif"
}
