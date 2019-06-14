package org.globalforestwatch.gladalerts

import cats.implicits._
import geotrellis.contrib.polygonal._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.globalforestwatch.util.AnalysisRDD
import org.globalforestwatch.features.GADMFeatureId

object GladAlertsRDD extends AnalysisRDD {

  type SOURCES = GladAlertsGridSources
  type FEATUREID = GADMFeatureId
  type SUMMARY = GladAlertsSummary
  type TILE = GladAlertsTile

  def getSources(window: Extent): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      GladAlertsGrid.getRasterSource(window)
    }
  }

  def readWindow(rs: SOURCES,
                 window: Extent): Either[Throwable, Raster[TILE]] =
    rs.readWindow(window)

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options): SUMMARY = {
    raster.polygonalSummary(
      geometry = geometry,
      emptyResult = new GladAlertsSummary(),
      options = options
    )
  }

  def reduceSummarybyKey(
                          featuresWithSummaries: RDD[(FEATUREID, SUMMARY)]
                        ): RDD[(FEATUREID, SUMMARY)] = {
    featuresWithSummaries.reduceByKey {
      case (summary1, summary2) =>
        summary1.merge(summary2)
    }

  }

}
