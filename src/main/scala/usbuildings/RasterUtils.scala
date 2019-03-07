package usbuildings

import geotrellis.contrib.vlm.RasterSource
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.raster.{FloatUserDefinedNoDataCellType, MultibandTile, Raster, Tile, TileLayout, isData}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.Extent

object RasterUtils {

  val rasterFileGrid: LayoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(
      layoutCols = 36, layoutRows = 18,
      tileCols = 40000, tileRows = 40000)
    LayoutDefinition(worldExtent, tileLayout)
  }

  val tileGrid: LayoutDefinition = {
    val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
    val tileLayout = TileLayout(
      layoutCols = 3600, layoutRows = 1800,
      tileCols = 400, tileRows = 400)
    LayoutDefinition(worldExtent, tileLayout)
  }

  def gridToRasterSources(grid: String): (RasterSource, RasterSource, RasterSource) = {
    (
      GeoTiffRasterSource(s"s3://gfw2-data/forest_change/hansen_2018/${grid}.tif"),
      GeoTiffRasterSource(s"s3://gfw2-data/forest_cover/2000_treecover/Hansen_GFC2014_treecover2000_${grid}.tif"),
      GeoTiffRasterSource(s"s3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/${grid}_t_aboveground_biomass_ha_2000.tif")
    )
  }

  def rasterTiles(
    loss_rs: RasterSource,
    tcd_rs: RasterSource,
    co2_rs: RasterSource
  ): Iterator[(Raster[MultibandTile], Raster[MultibandTile], Raster[MultibandTile])] = {

    val loss_tiled = loss_rs.tileToLayout(tileGrid)
    val tcd_tiled = tcd_rs.tileToLayout(tileGrid)
    val co2_tiled = tcd_rs.tileToLayout(tileGrid)

    val keys = loss_tiled.keys

    keys.toIterator.map { key =>
      val extent: Extent = key.extent(tileGrid)
      val loss_year = loss_tiled.read(key).get
      val tcd = tcd_tiled.read(key).get
      val co2 = co2_tiled.read(key).get
      (Raster(loss_year, extent), Raster(tcd, extent), Raster(co2, extent))
    }
  }

  def rasterAsTable(
    loss_raster: Raster[MultibandTile],
    tcd_raster: Raster[MultibandTile],
    co2_raster: Raster[MultibandTile]
  ): Vector[(Int, Int, Int, Int, Int)] = {
    val loss_tile: MultibandTile = loss_raster.tile.interpretAs(FloatUserDefinedNoDataCellType(0))
    val loss: Tile = loss_tile.band(0)

    val tcd_tile: MultibandTile = tcd_raster.tile
    val tcd: Tile = tcd_tile.band(0)

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

    table3
  }
}
