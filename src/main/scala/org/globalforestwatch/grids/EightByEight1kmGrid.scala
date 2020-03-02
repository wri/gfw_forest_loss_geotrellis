package org.globalforestwatch.grids

trait EightByEight1kmGrid[T <: GridSources] extends Grid[T] {
  val pixelSize = (90.0 / 9984.0)
  val gridSize = 90
  val rowCount = 10
  val blockSize = 416
}
