package org.globalforestwatch.layers

case class TotalCarbon2000(grid: String, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/total_carbon_2000/$model/$grid.tif"
}
