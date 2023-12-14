package org.globalforestwatch.layout

import geotrellis.raster.TileLayout
import geotrellis.layer.LayoutDefinition
import geotrellis.vector.Extent

trait RasterLayerGrid extends Serializable  {
  val extent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
  val pixelSize: Double
  val gridSize: Int
  val rowCount: Int
  val blockSize: Int

  /** this represents the tile layout of 10x10 degrees */
  @transient lazy val rasterFileGrid: LayoutDefinition = {
    val tileLayout = TileLayout(
      layoutCols = (extent.xmin.toInt until extent.xmax.toInt by gridSize).length,
      layoutRows = (extent.ymin.toInt until extent.ymax.toInt by gridSize).length,
      tileCols = math.round(gridSize / pixelSize).toInt,
      tileRows = math.round(gridSize / pixelSize).toInt
    )
    LayoutDefinition(extent, tileLayout)
  }

  /** Windows inside each tile for distributing the job IO
    * Matched to read GeoTiffs written with tiled segment layout
    */
  @transient lazy val segmentTileGrid: LayoutDefinition = {
    val tileLayout = TileLayout(
      layoutCols = (extent.xmin.toInt until extent.xmax.toInt by gridSize).length * (math
        .round(gridSize / pixelSize)
        .toInt / blockSize),
      layoutRows = (extent.ymin.toInt until extent.ymax.toInt by gridSize).length * (math
        .round(gridSize / pixelSize)
        .toInt / blockSize),
      tileCols = blockSize,
      tileRows = blockSize
    )
    LayoutDefinition(extent, tileLayout)
  }
}
