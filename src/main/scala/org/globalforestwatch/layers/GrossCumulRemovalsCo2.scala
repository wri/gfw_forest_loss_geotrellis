package org.globalforestwatch.layers

case class GrossCumulRemovalsCo2(grid: String, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/gross_cumul_removals_co2/$model/$grid.tif"
}
