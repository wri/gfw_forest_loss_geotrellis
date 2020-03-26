package org.globalforestwatch.grids

trait TenByTen30mGrid[T <: GridSources] extends Grid[T] {
  val pixelSize = 0.00025
  val gridSize = 10
  val rowCount = 40000
  val blockSize = 400
}
