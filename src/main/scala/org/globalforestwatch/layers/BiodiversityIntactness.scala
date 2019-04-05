package org.globalforestwatch.layers

class BiodiversityIntactness(grid: String) extends DIntegerLayer with OptionalDLayer {
  val uri: String = s"$basePath/biodiversity_intactness/$grid.tif"

  def lookup(value: Double): Integer =
    if (value == 0) null
    else if (value > 0) 100
    else if (value > -4) 90
    else if (value > -8) 80
    else if (value > -12) 70
    else if (value > -16) 60
    else if (value > -20) 50
    else if (value > -24) 40
    else if (value > -28) 30
    else if (value > -32) 20
    else if (value > -36) 10
    else 0

}