package org.globalforestwatch.stats.afi

import cats.Monoid
import geotrellis.raster.{RasterExtent, Tile}
import org.globalforestwatch.raster._
import org.globalforestwatch.stats.Analyzer
import org.globalforestwatch.util.Geodesy

object AFiAnalyzer extends Analyzer[AFiSummary] {
  val layers = List(
    TreeCoverLoss,
    SBTNNaturalForests,
    NegligibleRisk,
    GadmAdm0,
    GadmAdm1,
    GadmAdm2
  )

  def update(tiles: Seq[Tile], re: RasterExtent, acc: AFiSummary)(col: Int, row: Int)(implicit ev: Monoid[AFiSummary]): Unit = {
    val Seq(tclTile, natForestTile, negRiskTile, gadm0Tile, gadm1Tile, gadm2Tile) = tiles

    // Read from tiles for this pixel
    val lossYear: Option[Int] = TreeCoverLoss.convert(tclTile, col, row)

    val gadm0 = Option(gadm0Tile).map(GadmAdm0.convert(_, col, row)).getOrElse("")
    val gadm1 = Option(gadm1Tile).flatMap(GadmAdm1.convert(_, col, row)).getOrElse(null)
    val gadm2 = Option(gadm2Tile).flatMap(GadmAdm2.convert(_, col, row)).getOrElse(null)
    val gadmId: String = s"$gadm0.$gadm1.$gadm2"

    val naturalForestCategory: String =
      Option(natForestTile).flatMap(SBTNNaturalForests.convert(_,col, row)).getOrElse("Unknown")
    val isNaturalForest = naturalForestCategory == "Natural Forest"

    val negligibleRisk: String = Option(negRiskTile).map(NegligibleRisk.convert(_, col, row)).getOrElse("Unknown")

    // pixel Area
    val lat: Double = re.gridRowToMap(row)
    val area: Double = Geodesy.pixelArea(
      lat,
      re.cellSize
    )
    val areaHa = area / 10000.0

    // Update summary data
    // NOTE: Not clear that this properly deals with missing data, but that was
    // pre-existing in the original AFi code
    val summaryData = acc.stats.getOrElseUpdate(gadmId, Monoid[AFiData].empty)
    summaryData.total_area__ha += areaHa

    if (negligibleRisk == "NO") {
      summaryData.negligible_risk_area__ha += areaHa
    }

    if (naturalForestCategory == "Natural Forest") {
      summaryData.natural_forest__extent += areaHa
    }

    if (lossYear.map(_ >= 2021).getOrElse(false) && isNaturalForest) {
      summaryData.natural_forest_loss__ha += areaHa
    }
  }
}
