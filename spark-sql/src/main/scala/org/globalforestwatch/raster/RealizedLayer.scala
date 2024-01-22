package org.globalforestwatch.raster

import geotrellis.layer._
import geotrellis.raster.Tile
import geotrellis.raster.RasterSource
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.layout.RasterLayerGrid
import org.globalforestwatch.util.Maybe
import org.log4s.getLogger

case class RealizedLayer(
  layer: RasterLayer,
  grid: RasterLayerGrid,
  catalog: String = "default"
) {
  val uriTemplate = GfwConfig.read(catalog).rasterCatalog.getSourceUri(layer.name)
  val logger = getLogger(getClass)

  def uri(key: SpatialKey): String = {
    val layout = grid.rasterFileGrid
    val extent = layout.mapTransform.keyToExtent(key)
    val col = math.floor(extent.xmin).toInt
    val row = math.ceil(extent.ymax).toInt
    val lng: String = if (col >= 0) f"$col%03dE" else f"${-col}%03dW"
    val lat: String = if (row >= 0) f"$row%02dN" else f"${-row}%02dS"

    val tileId = s"${lat}_${lng}"

    uriTemplate
      .replace("{grid_size}", grid.gridSize.toString)
      .replace("{row_count}", grid.rowCount.toString)
      .replace("{tile_id}", tileId)
  }

  def read(key: SpatialKey): Maybe[Tile] = {
    val target = uri(key)
    logger.warn(s"Reading tile for ${key} at URI=${target}")
    val source = LayoutTileSource.spatial(RasterSource(target), grid.segmentTileGrid)
    val tile = source.read(key, Seq(0)).map(_.band(0))
    (layer.required, tile) match {
      case (true, None) => Maybe.error(f"Required layer ${layer.name} missing at ${key}")
      case (true, Some(t)) => Maybe(t)
      case (_, opt) => Maybe(opt, null) // This is a bit sketchy; Maybe isn't meant for TVL
    }
  }
}
