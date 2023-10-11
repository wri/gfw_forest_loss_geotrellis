package org.globalforestwatch.summarystats.afi

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.Raster
import geotrellis.raster.summary.GridVisitor
import org.globalforestwatch.summarystats.{Summary, summarySemigroup}
import org.globalforestwatch.util.Geodesy

import java.time.LocalDate

/** LossData Summary by year */
case class AFiSummary(
                                   stats: Map[AFiDataGroup, AFiData] = Map.empty
                                 ) extends Summary[AFiSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: AFiSummary): AFiSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    AFiSummary(stats.combine(other.stats))
  }

  def isEmpty = stats.isEmpty
}

object AFiSummary {

  def getGridVisitor(kwargs: Map[String, Any]): GridVisitor[Raster[AFiTile], AFiSummary] =
    new GridVisitor[Raster[AFiTile], AFiSummary] {
      private var acc: AFiSummary =
        new AFiSummary()

      def result: AFiSummary = acc

      def visit(raster: Raster[AFiTile], col: Int, row: Int): Unit = {
        val lossYear: Integer = raster.tile.treeCoverLoss.getData(col, row)
        val naturalForestCategory: String = raster.tile.sbtnNaturalForest.getData(col, row)
        val negligibleRisk: String = raster.tile.negligibleRisk.getData(col, row)

       val gadmAdm0: String = raster.tile.gadmAdm0.getData(col, row)
       val gadmAdm1: Integer = raster.tile.gadmAdm1.getData(col, row)
       val gadmAdm2: Integer = raster.tile.gadmAdm2.getData(col, row)
       val gadmId: String = s"$gadmAdm0.$gadmAdm1.$gadmAdm2"

        // pixel Area
        val lat: Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(
          lat,
          raster.cellSize
        )
        val areaHa = area / 10000.0
        val isNaturalForest = naturalForestCategory == "Natural Forest"


        val groupKey = AFiDataGroup(gadmId)
        val summaryData = acc.stats.getOrElse(groupKey, AFiData(0, 0, 0, 0))
        summaryData.total_area__ha += areaHa

        if (negligibleRisk == "YES") {
          summaryData.negligible_risk_area__ha += areaHa
        }

        if (naturalForestCategory == "Natural Forest") {
          summaryData.natural_forest__extent += areaHa
        }

        if (lossYear >= 2021 && naturalForestCategory == "Natural Forest") {
          summaryData.natural_forest_loss__ha += areaHa
        }

        val new_stats = acc.stats.updated(groupKey, summaryData)
        acc = AFiSummary(new_stats)
      }
    }
}
