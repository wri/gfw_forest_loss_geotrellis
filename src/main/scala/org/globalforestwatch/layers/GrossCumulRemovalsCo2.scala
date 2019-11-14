package org.globalforestwatch.layers

case class GrossCumulRemovalsCo2(grid: String, model: String="standard")
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/gross_cumul_removals_co2/$model/$grid.tif"
}
