package usbuildings

import geotrellis.contrib.vlm.RasterSource
import geotrellis.contrib.vlm.gdal.GDALRasterSource
import geotrellis.raster.{CellSize, RasterExtent}
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.{Extent, Geometry}

object Terrain {

  val terrainTilesSkadiGrid: LayoutDefinition = {
    val nedRasterExtent = RasterExtent(
      Extent(-180.0000, -90.0000, 180.0000, 90.0000),
      CellSize(1, 1))
    LayoutDefinition(nedRasterExtent, tileCols = 3601, tileRows = 3601)
  }

  /** Skadi raster for each a 1x1 degree tile */
  def tileUri(key: SpatialKey): String = {
    val lat: String = if (key.row < 90) f"N${89 - key.row}%02d" else f"S${-89 + key.row}%02d"
    val long: String = if (key.col < 180) f"E${179 - key.col}%02d" else f"W${-179 + key.col}%02d"
    f"/vsigzip//vsis3/elevation-tiles-prod/v2/skadi/$lat/$lat$long.hgt.gz"
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
