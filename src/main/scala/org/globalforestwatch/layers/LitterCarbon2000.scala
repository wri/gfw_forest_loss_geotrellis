package org.globalforestwatch.layers

case class LitterCarbon2000(grid: String, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/litter_carbon_2000/$model/$grid.tif"
}
