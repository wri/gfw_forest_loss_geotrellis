package org.globalforestwatch.summarystats.firealerts

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD
import org.globalforestwatch.util.Util.getAnyMapValue


object FireAlertsRDD extends SummaryRDD {

  type SOURCES = FireAlertsGridSources
  type SUMMARY = FireAlertsSummary
  type TILE = FireAlertsTile

  def getSources(windowKey: SpatialKey, windowLayout: LayoutDefinition, kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")

    Either.catchNonFatal {
      fireAlertType match {
        case "viirs" => ViirsGrid.getRasterSource(windowKey, windowLayout, kwargs)
        case "modis" => ModisGrid.getRasterSource(windowKey, windowLayout, kwargs)
      }
    }
  }

  def readWindow(rs: SOURCES, windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[TILE]] =
    rs.readWindow(windowKey, windowLayout)

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          kwargs: Map[String, Any]): PolygonalSummaryResult[SUMMARY] = {
    raster.polygonalSummary(
      geometry,
      FireAlertsSummary.getGridVisitor(kwargs),
      options = options
    )
  }
}
