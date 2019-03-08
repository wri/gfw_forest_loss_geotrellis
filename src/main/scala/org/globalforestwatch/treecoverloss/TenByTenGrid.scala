package org.globalforestwatch.treecoverloss

import geotrellis.vector.Point
import geotrellis.raster.TileLayout
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.Extent

object TenByTenGrid {

  /** this represents the tile layout of 10x10 degrees */
  val rasterFileGrid: LayoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(
      layoutCols = 36, layoutRows = 18,
      tileCols = 40000, tileRows = 40000)
    LayoutDefinition(worldExtent, tileLayout)
  }

  /** Windows inside each 10x10 tile for distributing the job IO
    * Matched to read GeoTiffs written with striped segment layout
    */
  val stripedTileGrid: LayoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(
      layoutCols = 36, layoutRows = 18 * (40000 / 10),
      tileCols = 40000, tileRows = 10)
    LayoutDefinition(worldExtent, tileLayout)
  }
  /** Windows inside each 10x10 tile for distributing the job IO
    * Matched to read GeoTiffs written with tiled segment layout
    */
  val blockTileGrid: LayoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(
      layoutCols = 3600, layoutRows = 1800,
      tileCols = 400, tileRows = 400)
    LayoutDefinition(worldExtent, tileLayout)
  }

  /** Translate from a point on a map to file grid ID of 10x10 grid
    * Top-Left corner, exclusive on south, east, inclusive on north and west
    */
  def pointGridId(point: Point): String = {
    val col = (math.floor(point.y / 10).toInt * 10) + 10
    val long: String = if (col >= 0) f"${col}%03dE" else f"${-col}%03dW"

    val row = (math.floor(point.x / 10).toInt * 10)
    val lat: String = if (row >= 0) f"${row}%02dN" else f"${-row}%02dS"

    s"${lat}_${long}"
  }

  def getRasterSource(windowExtent: Extent): TenByTenGridSources = {
    val gridId = pointGridId(windowExtent.center)
    val source = TenByTenGridSources(gridId)

    // NOTE: This check will cause an eager fetch of raster metadata
    require(source.forestChangeSource.extent.intersects(windowExtent),
      s"${source.forestChangeSource.uri} does not intersect: $windowExtent")
    require(source.bioMassSource.extent.intersects(windowExtent),
      s"${source.bioMassSource.uri} does not intersect: $windowExtent")
    require(source.treeCoverSource.extent.intersects(windowExtent),
      s"${source.treeCoverSource.uri} does not intersect: $windowExtent")

    source
  }
}
