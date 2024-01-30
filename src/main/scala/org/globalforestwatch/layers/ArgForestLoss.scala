package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

// Argentina government forest loss data gives a forest loss year or a range of years
// for the forest loss (such as 2013-2017). For a range, we record only the final
// year of the range, but mark it as approximate in the rasterization by adding 100
// to the usual representation (year - 2000). The output values of this layer are
// ApproxYear(year, approx), where approx is true if the year is approximate (i.e. is
// the last year of a range).
case class ArgForestLoss(gridTile: GridTile, kwargs: Map[String, Any])
  extends ApproxYearLayer
    with OptionalILayer {
  val datasetName = "arg_otbn_forest_loss"
  val uri: String =
    uriForGrid(gridTile, kwargs)

  override def lookup(value: Int): ApproxYear =
    if (value == 0) ApproxYear(0, false)
    else if (value < 100)
      ApproxYear(2000+value, false)
    else
      ApproxYear(2000+(value-100), true)
}
