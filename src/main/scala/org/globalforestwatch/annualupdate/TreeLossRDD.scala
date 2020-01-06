package org.globalforestwatch.annualupdate

import cats.implicits._
import geotrellis.contrib.polygonal._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.globalforestwatch.util.AnalysisRDD
import org.globalforestwatch.features.GadmFeatureId

object TreeLossRDD extends AnalysisRDD {

  type SOURCES = TreeLossGridSources
  type FEATUREID = GadmFeatureId
  type SUMMARY = TreeLossSummary
  type TILE = TreeLossTile

  def getSources(window: Extent): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      TreeLossGrid.getRasterSource(window)
    }
  }

  def readWindow(rs: SOURCES,
                 window: Extent): Either[Throwable, Raster[TILE]] =
    rs.readWindow(window)

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          tcdYear: Int = 2000): SUMMARY = {
    raster.polygonalSummary(
      geometry = geometry,
      emptyResult = new TreeLossSummary(),
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
