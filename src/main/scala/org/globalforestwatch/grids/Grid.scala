package org.globalforestwatch.grids

import geotrellis.raster.TileLayout
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.{Extent, Point}
import org.globalforestwatch.layers.{OptionalLayer, RequiredLayer}

trait Grid[T <: GridSources] {

  val gridExtent: Extent
  val pixelSize: Double
  val gridSize: Int
  val rowCount: Int
  val blockSize: Int

  /** this represents the tile layout of 10x10 degrees */
  lazy val rasterFileGrid: LayoutDefinition = {
    val tileLayout = TileLayout(
      layoutCols = (gridExtent.xmin until gridExtent.xmax by gridSize).length,
      layoutRows = (gridExtent.ymin until gridExtent.ymax by gridSize).length,
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
      layoutCols = (gridExtent.xmin until gridExtent.xmax by gridSize).length,
      layoutRows = (gridExtent.ymin until gridExtent.ymax by gridSize).length * (math
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
      layoutCols = (gridExtent.xmin until gridExtent.xmax by gridSize).length * (math
        .round(gridSize / pixelSize)
        .toInt / blockSize),
      layoutRows = (gridExtent.ymin until gridExtent.ymax by gridSize).length * (math
        .round(gridSize / pixelSize)
        .toInt / blockSize),
      tileCols = blockSize,
      tileRows = blockSize
    )
    LayoutDefinition(gridExtent, tileLayout)
  }

  def getSources(gridId: String): T

  def checkSources(gridId: String, windowExtent: Extent): T

  // NOTE: This check will cause an eager fetch of raster metadata
  def checkRequired(layer: RequiredLayer, windowExtent: Extent): Unit = {
    require(
      layer.extent.intersects(windowExtent),
      s"${layer.uri} does not intersect: $windowExtent"
    )
  }

  // Only check these guys if they're defined
  def checkOptional(layer: OptionalLayer, windowExtent: Extent): Unit = {
    layer.extent.foreach { extent =>
      require(
        extent.intersects(windowExtent),
        s"${layer.uri} does not intersect: $windowExtent"
      )
    }
  }

  def getRasterSource(windowExtent: Extent): T = {
    val gridId = GridId.pointGridId(windowExtent.center, gridSize)
    checkSources(gridId, windowExtent: Extent)
  }
}
