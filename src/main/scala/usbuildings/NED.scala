package usbuildings

import geotrellis.contrib.vlm.RasterSource
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.raster.{CellSize, RasterExtent, TileLayout}
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.{Extent, Geometry}

object NED {

  /** Tile grid over WGS84 of one degree tiles.
    *
    * This object generates SpatialKeys that refer to these tiles.
    * SpatialKey can either identify a specific tile or a tile-shaped are for which work can be done.
    */
  val tileGrid: LayoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(
      layoutCols = 360, layoutRows = 180,
      tileCols = 10812, tileRows = 10812)
    LayoutDefinition(worldExtent, tileLayout)
  }

  /** NED raster for each a 1x1 degree tile */
  def tileUri(key: SpatialKey): String = {
    val col = key.col - 180
    val long: String = if (col >= 0) f"E${col}%03d" else f"W${-col}%03d"

    val row = 90 - key.row
    val lat: String = if (row >= 0) f"N${row}%02d" else f"S${-row}%02d"

    f"s3://azavea-datahub/raw/ned-13arcsec-geotiff/img${lat.toLowerCase}${long.toLowerCase}_13.tif"
  }

  def getRasterSource(tileKey: SpatialKey): RasterSource = {
    val uri = tileUri(tileKey)
    val keyExtent = tileKey.extent(tileGrid)
    val rasterSource = GeoTiffRasterSource(uri)

    // NOTE: This check will cause an eager fetch of raster metadata
    require(rasterSource.extent.intersects(keyExtent),
      s"$uri does not intersect $tileKey: $keyExtent")

    rasterSource
  }

  def tileRasterExtent(tileKey: SpatialKey): RasterExtent = {
    RasterExtent(
      extent = tileKey.extent(tileGrid),
      cellwidth = tileGrid.cellSize.width,
      cellheight = tileGrid.cellSize.height,
      cols = tileGrid.tileCols,
      rows = tileGrid.tileRows)
  }

  def getRasterSource(geom: Geometry): Seq[RasterSource] = {
    val tileKeys = tileGrid.mapTransform.keysForGeometry(geom)
    tileKeys.toSeq.map({ key => getRasterSource(key) })
  }
}
