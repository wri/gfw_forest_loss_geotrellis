package org.globalforestwatch.treecoverloss

import geotrellis.contrib.vlm.geotiff._
import geotrellis.proj4._
import geotrellis.raster.{RasterExtent, TileLayout, _}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.Extent
import org.scalatest._

class RasterSourceSpec extends FunSpec with Matchers {

  val rs = GeoTiffRasterSource("s3://gfw2-data/forest_change/hansen_2018/50N_080W.tif")
  val rs2 = GeoTiffRasterSource("s3://gfw2-data/forest_cover/2000_treecover/Hansen_GFC2014_treecover2000_50N_080W.tif")
  val rs3 = GeoTiffRasterSource("s3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/50N_080W_t_aboveground_biomass_ha_2000.tif")

  ignore("calculate stats") {
    val loss_raster: Raster[MultibandTile] = rs.read(Extent(-72.97754892, 43.85921846, -72.80676639, 43.97153490)).get
    val loss_tile: MultibandTile = loss_raster.tile.interpretAs(FloatUserDefinedNoDataCellType(0))
    val loss: Tile = loss_tile.band(0)


    val tcd_raster: Raster[MultibandTile] = rs2.read(Extent(-72.97754892, 43.85921846, -72.80676639, 43.97153490)).get
    val tcd_tile: MultibandTile = tcd_raster.tile
    val tcd: Tile = tcd_tile.band(0)

    val co2_raster: Raster[MultibandTile] = rs3.read(Extent(-72.97754892, 43.85921846, -72.80676639, 43.97153490)).get
    val co2_tile: MultibandTile = co2_raster.tile
    val co2: Tile = co2_tile.band(0)

    var table = Vector.empty[(Int, Int, Int)]
    loss.foreach { (col: Int, row: Int, v: Int) =>
      if (isData(v)) {
        val tup = (col, row, v)
        table = table :+ tup
      }
    }

    val table2: Vector[(Int, Int, Int, Int)] =
      table.map { case (col, row, v) =>
        val v2 = tcd.get(col, row)
        (col, row, v, v2)
      }

    val table3: Vector[(Int, Int, Int, Int, Int)] =
      table2.map { case (col, row, v, v2) =>
        val v3 = co2.get(col, row)
        (col, row, v, v2, v3)
      }

    info(s"Loss values ${table3.toList}")
  }
  it("can access basic metadata") {
    rs.crs
    rs.extent
    rs.dimensions
  }

  it("can read a window from raster") {
    val extent  = Extent(-72.97754892, 43.85921846, -72.80676639, 43.97153490)
    val geom = extent.buffer(-0.01).toPolygon()
    val raster = rs.read(Extent(-72.97754892, 43.85921846, -72.80676639, 43.97153490)).get
    val histo = raster.tile.polygonalHistogram(raster.extent, geom)
    // val histo: Array[Histogram[Int]] = raster.tile.histogram
    val binCounts = histo(0).binCounts()
    val Some((min, max)) = histo(0).minMaxValues()

    info(s"Extent is ${raster.extent}")
    info(s"Dimensions are ${raster.dimensions}")
    info(s"Minimum value is $min")
    info(s"Maximum value is $max")
    info(s"Bin Count is ${binCounts}")
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

    val tileSource = rs.tileToLayout(tileGrid)

    val keys = tileSource.keys
    val (rasterKey, raster) = tileSource.readAll.next
    val (regionKey, region) = tileSource.keyedRasterRegions.next
    info(s"Tile Count: ${keys.size}")
    info(s"$rasterKey - $raster")
    info(s"$regionKey - $region")
  }
}