package org.globalforestwatch.summarystats.afi

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.Raster
import geotrellis.raster.summary.GridVisitor
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy
import java.time.LocalDate

/** LossData Summary by year */
case class AFiSummary(
                                   stats: Map[AFiRawDataGroup, AFiRawData] = Map.empty
                                 ) extends Summary[AFiSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: AFiSummary): AFiSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    AFiSummary(stats.combine(other.stats))
  }
  def isEmpty = stats.isEmpty

  def toAFiData(): AFiData = {
    stats
      .map { case (group, data) => group.toAFiData(data.alertCount, data.treeCoverExtentArea) }
      .foldLeft(AFiData.empty)( _ merge _)
  }
}

object AFiSummary {

  def getGridVisitor(kwargs: Map[String, Any]): GridVisitor[Raster[AFiTile], AFiSummary] =
    new GridVisitor[Raster[AFiTile], AFiSummary] {
      private var acc: AFiSummary =
        new AFiSummary()

      def result: AFiSummary = acc

      def visit(raster: Raster[AFiTile], col: Int, row: Int): Unit = {
        val lossYear: Integer = raster.tile.treeCoverLoss.getData(col, row)

        val iso = ...
        val adm1 = ...
        val adm2: Integer = ...

        val groupKey = AFiRawDataGroup(iso, adm1, adm2, lossYear)
        val area = ...

        val summaryData = acc.stats.getOrElse(groupKey, AFiRawData(lossArea = 0))
        summaryData.lossArea += area

        val new_stats = acc.stats.updated(groupKey, summaryData)
        acc = AFiSummary(new_stats)
      }
    }
}
