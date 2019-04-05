package org.globalforestwatch.layers

class KeyBiodiversityAreas(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"$basePath/kba/$grid.tif"
}
