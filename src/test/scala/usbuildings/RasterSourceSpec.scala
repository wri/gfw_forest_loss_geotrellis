package usbuildings

import org.scalatest._
import geotrellis.contrib.vlm.LayoutTileSource
import geotrellis.contrib.vlm.geotiff._
import geotrellis.proj4._
import geotrellis.vector.Extent
import geotrellis.raster.RasterExtent
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.raster.TileLayout

class RasterSourceSpec extends FunSpec with Matchers {

  val rs = GeoTiffRasterSource("s3://azavea-datahub/raw/ned-13arcsec-geotiff/imgn44w073_13.tif")

  it("can access basic metadata") {
    rs.crs
    rs.extent
    rs.dimensions
  }

  it("can read a window from raster") {
    val raster = rs.read(Extent(-72.97754892, 43.85921846, -72.80676639, 43.97153490)).get
    info(s"${raster.extent}")
    info(s"${raster.dimensions}")
  }

  it("can read multiple windows at a time") {
    rs.readExtents(
      List(
        Extent(-72.97531271,43.92968549,-72.91916503,43.96661144),
        Extent(-72.87748423,43.85790751,-72.82133655,43.89483346)))
  }

  it("can generate reproject view") {
    val rsWM = rs.reproject(WebMercator)
    info(s"${rsWM.crs}")
    info(s"${rsWM.extent}")
  }

  it("can match pixel layout of another raster") {
    val extent = Extent(-8126384.672071017, 5311890.756776384, -8014939.525327434, 5465528.157226967)
    val rasterExtent = RasterExtent(extent, cols = 10000, rows = 10000)
    rs.reprojectToGrid(WebMercator, rasterExtent)
  }


  val layoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(
      layoutCols = 360, layoutRows = 180,
      tileCols = 10812, tileRows = 10812)
    LayoutDefinition(worldExtent, tileLayout)
  }

  it("can generate tiles to LayoutDefinition") {
    val tileGrid = {
      val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
      val tileLayout = TileLayout(
        layoutCols = 360 * 51, layoutRows = 180 * 51,
        tileCols = 10812 / 51, tileRows = 10812 / 51)
      LayoutDefinition(worldExtent, tileLayout)
    }

    val tileSource = LayoutTileSource(rs, tileGrid)

    val keys = tileSource.keys
    val (rasterKey, raster) = tileSource.readAll.next
    val (regionKey, region) = tileSource.keyedRasterRegions.next
    info(s"Tile Count: ${keys.size}")
    info(s"$rasterKey - $raster")
    info(s"$regionKey - $region")
  }
}