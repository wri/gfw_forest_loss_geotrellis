package org.globalforestwatch.layers

case class LitterCarbon2000(grid: String)
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/litter_carbon_2000/$grid.tif"
}
