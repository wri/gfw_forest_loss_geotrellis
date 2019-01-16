package usbuildings

import geotrellis.raster.histogram.{Histogram, StreamingHistogram}
import geotrellis.raster.{PixelIsArea, Raster, Tile, isData}
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.summary.polygonal.TilePolygonalSummaryHandler
import geotrellis.vector.Polygon

object CustomDoubleHistogramSummary extends TilePolygonalSummaryHandler[Histogram[Double]] {
  def handlePartialTile(raster: Raster[Tile], polygon: Polygon): Histogram[Double] = {
    val Raster(tile, _) = raster
    val rasterExtent = raster.rasterExtent
    val histogram = StreamingHistogram()
    // include options so we can count partial pixel overlap
    val options = Rasterizer.Options(includePartial =  true, sampleType = PixelIsArea)
    Rasterizer.foreachCellByGeometry(polygon, rasterExtent, options)  { (col: Int, row: Int) =>
      val z = tile.getDouble(col, row)
      if (isData(z)) histogram.countItem(z, 1)
    }
    histogram
  }

  def handleFullTile(tile: Tile): Histogram[Double] = {
    val histogram = StreamingHistogram()
    tile.foreach { (z: Int) => if (isData(z)) histogram.countItem(z, 1) }
    histogram
  }

  def combineResults(rs: Seq[Histogram[Double]]): Histogram[Double] =
    if (rs.nonEmpty) rs.reduce(_ merge _)
    else StreamingHistogram()
}