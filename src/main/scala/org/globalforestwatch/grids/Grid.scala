package org.globalforestwatch.grids

import geotrellis.raster.TileLayout
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.vector.Extent

trait Grid[T <: GridSources] {

  val gridExtent: Extent
  val pixelSize: Double
  val gridSize: Int
  val rowCount: Int
  val blockSize: Int

  /** this represents the tile layout of 10x10 degrees */
  lazy val rasterFileGrid: LayoutDefinition = {
    val tileLayout = TileLayout(
      layoutCols = (gridExtent.xmin.toInt until gridExtent.xmax.toInt by gridSize).length,
      layoutRows = (gridExtent.ymin.toInt until gridExtent.ymax.toInt by gridSize).length,
      tileCols = math.round(gridSize / pixelSize).toInt,
      tileRows = math.round(gridSize / pixelSize).toInt
    )
    LayoutDefinition(gridExtent, tileLayout)
  }

  /** Windows inside each tile for distributing the job IO
    * Matched to read GeoTiffs written with striped segment layout
    */
  lazy val stripedTileGrid: LayoutDefinition = {
    val tileLayout = TileLayout(
      layoutCols = (gridExtent.xmin.toInt until gridExtent.xmax.toInt by gridSize).length,
      layoutRows = (gridExtent.ymin.toInt until gridExtent.ymax.toInt by gridSize).length * (math
        .round(gridSize / pixelSize)
        .toInt / rowCount),
      tileCols = math.round(gridSize / pixelSize).toInt,
      tileRows = rowCount
    )
    LayoutDefinition(gridExtent, tileLayout)
  }

  /** Windows inside each tile for distributing the job IO
    * Matched to read GeoTiffs written with tiled segment layout
    */
  lazy val blockTileGrid: LayoutDefinition = {
    val tileLayout = TileLayout(
      layoutCols = (gridExtent.xmin.toInt until gridExtent.xmax.toInt by gridSize).length * (math
        .round(gridSize / pixelSize)
        .toInt / blockSize),
      layoutRows = (gridExtent.ymin.toInt until gridExtent.ymax.toInt by gridSize).length * (math
        .round(gridSize / pixelSize)
        .toInt / blockSize),
      tileCols = blockSize,
      tileRows = blockSize
    )
    LayoutDefinition(gridExtent, tileLayout)
  }

  // Get the set of grid sources (subclass of GridSources) associated with specified
  // grid tile and the configuration/catalog in kwargs.
  def getSources(gridTile: GridTile, kwargs:  Map[String, Any]): T

  // Get the set of grid sources (subclass of GridSources) associated with specified
  // windowKey and windowLayout and the configuration/catalog in kwargs.
  def getRasterSource(windowKey: SpatialKey, windowLayout: LayoutDefinition, kwargs:  Map[String, Any]): T = {
    val windowExtent: Extent = windowKey.extent(windowLayout)
    val gridId = GridId.pointGridId(windowExtent.center, gridSize)
    val gridTile = GridTile(gridSize, rowCount, blockSize, gridId)

    getSources(gridTile, kwargs)
  }
}
