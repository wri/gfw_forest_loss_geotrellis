package org.globalforestwatch.treecoverloss_simple_60

case class TreeLossYearData(year: Int, var area_loss: Double)

object TreeLossYearData {

  implicit object YearOrdering extends Ordering[TreeLossYearData] {
    def compare(a: TreeLossYearData, b: TreeLossYearData): Int =
      a.year compare b.year
  }

}
