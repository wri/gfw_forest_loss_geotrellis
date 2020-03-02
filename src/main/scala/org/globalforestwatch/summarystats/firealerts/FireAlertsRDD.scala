package org.globalforestwatch.summarystats.firealerts

import cats.implicits._
import geotrellis.contrib.polygonal._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD
import org.globalforestwatch.util.Util.getAnyMapValue


object FireAlertsRDD extends SummaryRDD {

  type SOURCES = FireAlertsGridSources
  type SUMMARY = FireAlertsSummary
  type TILE = FireAlertsTile

  def getSources(window: Extent, kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")

    Either.catchNonFatal {
      fireAlertType match {
        case "viirs" => ViirsGrid.getRasterSource(window, kwargs)
        case "modis" => ModisGrid.getRasterSource(window, kwargs)
      }
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
      emptyResult = new FireAlertsSummary(kwargs = kwargs),
      options = options
    )
  }

}
