package usbuildings

import geotrellis.contrib.vlm.RasterSource
import geotrellis.contrib.vlm.gdal.GDALRasterSource
import geotrellis.raster.{CellSize, RasterExtent, TileLayout}
import geotrellis.vector.Point
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.{Extent, Geometry}

object Terrain {

  val terrainTilesSkadiGrid: LayoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(layoutCols = 360, layoutRows = 180, tileCols = 3601, tileRows = 3601)
    LayoutDefinition(worldExtent, tileLayout)
  }

  /** Skadi raster for each a 1x1 degree tile */
  def tileUri(key: SpatialKey): String = {
    // col: 0 = W179, col: 179 = W001, col: 180 = W000/E000
    val col = key.col - 180
    val long: String = if (col >= 0) f"E${col}%03d" else f"W${-col}%03d"

    val row = 89 - key.row
    val lat: String = if (row >= 0) f"N${row}%02d" else f"S${-row}%02d"

    f"s3://elevation-tiles-prod/v2/skadi/$lat/$lat$long.hgt.gz"
  }

  def getRasterSource(tileKey: SpatialKey): RasterSource = {
    GDALRasterSource(tileUri(tileKey))
  }

  def getRasterSource(tileKeys: Set[SpatialKey]): Seq[RasterSource] = {
    tileKeys.toSeq.map({ key => GDALRasterSource(tileUri(key)) })
  }

  def getRasterSource(geom: Geometry): Seq[RasterSource] = {
    val tileKeys = terrainTilesSkadiGrid.mapTransform.keysForGeometry(geom)
    tileKeys.toSeq.map({ key => GDALRasterSource(tileUri(key)) })
  }
}
