package org.globalforestwatch.grids

import geotrellis.raster.TileLayout
import geotrellis.layer.{LayoutDefinition, SpatialKey}
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

  def getSources(gridTile: GridTile, kwargs:  Map[String, Any]): T

  def checkSources(gridTile: GridTile, windowExtent: Extent, windowKey: SpatialKey, windowLayout: LayoutDefinition, kwargs:  Map[String, Any]): T = {

    def ccToMap(cc: AnyRef): Map[String, Any] =
      cc.getClass.getDeclaredFields.foldLeft(Map.empty[String, Any]) { (a, f) =>
        f.setAccessible(true)
        a + (f.getName -> f.get(cc))
      }

    val sources: T = getSources(gridTile, kwargs)

    val sourceMap = ccToMap(sources)

    for ((k, v) <- sourceMap) {

      v match {
        case s: RequiredLayer => checkRequired(s, windowExtent)
        case s: OptionalLayer => checkOptional(s, windowExtent)
        case _ => Unit
      }
    }

    sources
  }

  // NOTE: This check will cause an eager fetch of raster metadata
  def checkRequired(layer: RequiredLayer, windowExtent: Extent): Unit = {
    require(
      layer.source.extent.intersects(windowExtent),
      s"${layer.uri} does not intersect: $windowExtent"
    )
  }

  // Only check these guys if they're defined
  def checkOptional(layer: OptionalLayer, windowExtent: Extent): Unit = {
    layer.source.foreach { source =>
      require(
        source.extent.intersects(windowExtent),
        s"${layer.uri} does not intersect: $windowExtent"
      )
    }
  }

  def getRasterSource(windowKey: SpatialKey, windowLayout: LayoutDefinition, kwargs:  Map[String, Any]): T = {
    val windowExtent: Extent = windowKey.extent(windowLayout)
    val gridId = GridId.pointGridId(windowExtent.center, gridSize)
    val gridTile = GridTile(gridSize, rowCount, blockSize, gridId)

    checkSources(gridTile, windowExtent: Extent, windowKey: SpatialKey, windowLayout: LayoutDefinition, kwargs:  Map[String, Any])
  }
}
