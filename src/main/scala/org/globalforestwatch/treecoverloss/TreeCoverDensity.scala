package org.globalforestwatch.treecoverloss

object TreeCoverDensity {
  def threshold(density: Int): Int = {
    if (density <= 10) 0
    else if (density <= 15) 10
    else if (density <= 20) 15
    else if (density <= 25) 20
    else if (density <= 30) 25
    else if (density <= 50) 30
    else if (density <= 75) 50
    else 75
  }
}
