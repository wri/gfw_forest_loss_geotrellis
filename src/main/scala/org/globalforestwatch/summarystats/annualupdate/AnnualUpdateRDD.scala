package org.globalforestwatch.summarystats.annualupdate

import cats.implicits._
import geotrellis.contrib.polygonal._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object AnnualUpdateRDD extends SummaryRDD {

  type SOURCES = AnnualUpdateGridSources
  type SUMMARY = AnnualUpdateSummary
  type TILE = AnnualUpdateTile

  def getSources(window: Extent): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      AnnualUpdateGrid.getRasterSource(window)
    }
  }

  def readWindow(rs: SOURCES, window: Extent): Either[Throwable, Raster[TILE]] =
    rs.readWindow(window)

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          tcdYear: Int = 2000): SUMMARY = {
    raster.polygonalSummary(
      geometry = geometry,
      emptyResult = new AnnualUpdateSummary(),
      options = options
    )
  }

}
