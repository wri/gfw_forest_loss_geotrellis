package org.globalforestwatch.layers

case class DeadwoodCarbon2000(grid: String, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/deadwood_carbon_2000/$model/$grid.tif"
}
