package org.globalforestwatch.summarystats.annualupdate_minimal

import cats.implicits._
import geotrellis.contrib.polygonal._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object AnnualUpdateMinimalRDD extends SummaryRDD {

  type SOURCES = TreeLossGridSources
  type SUMMARY = AnnualUpdateMinimalSummary
  type TILE = AnnualUpdateMinimalTile

  def getSources(window: Extent): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      AnnualUpdateMinimalGrid.getRasterSource(window)
    }
  }

  def readWindow(rs: SOURCES, window: Extent): Either[Throwable, Raster[TILE]] =
    rs.readWindow(window)

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          kwargs: Map[String, Any]): SUMMARY = {
    raster.polygonalSummary(
      geometry = geometry,
      emptyResult = new AnnualUpdateMinimalSummary(),
      options = options
    )
  }

}
