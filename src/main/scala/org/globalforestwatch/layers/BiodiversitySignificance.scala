package org.globalforestwatch.layers

class BiodiversitySignificance(grid: String) extends DIntegerLayer with OptionalDLayer {
  val uri: String = s"$basePath/biodiversity_significance/$grid.tif"

  def lookup(value: Double): Integer =
    if (value > 6.3) 100
    else if (value > 3.03) 90
    else if (value > 1.76) 80
    else if (value > 1.23) 70
    else if (value > 0.676) 60
    else if (value > 0.34) 50
    else if (value > 0.245) 40
    else if (value > 0.191) 30
    else if (value > 0.144) 20
    else if (value > 0) 10
    else null

}