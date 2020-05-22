package org.globalforestwatch.grids

trait NinetyByNinety375mGrid[T <: GridSources] extends Grid[T] {
  val pixelSize = (90.0 / 27008.0)
  val gridSize = 90
  val rowCount = 27008
  val blockSize = 128
}
