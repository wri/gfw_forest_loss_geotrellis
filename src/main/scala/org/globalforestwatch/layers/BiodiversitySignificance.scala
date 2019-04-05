package org.globalforestwatch.layers

class BiodiversitySignificance(grid: String) extends DIntegerLayer with OptionalDLayer {
  val uri: String = s"s3://$basePath/biodiversity_significance/$grid.tif"

  def lookup(value: Double): Integer =
    if (value > 9) 100
    else if (value > 8) 90
    else if (value > 7) 80
    else if (value > 6) 70
    else if (value > 5) 60
    else if (value > 4) 50
    else if (value > 3) 40
    else if (value > 2) 30
    else if (value > 1) 20
    else if (value > 0) 10
    else null

}