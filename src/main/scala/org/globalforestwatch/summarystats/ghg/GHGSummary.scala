package org.globalforestwatch.summarystats.ghg

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.summary.GridVisitor
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.globalforestwatch.features.GfwProFeatureExtId
import scala.collection.mutable

/** GHGRawData broken down by GHGRawDataGroup, which includes the loss year and crop yield */
case class GHGSummary(
                                          stats: Map[GHGRawDataGroup,
                                            GHGRawData] = Map.empty
                                        ) extends Summary[GHGSummary] {

  /** Combine two Maps by combining GHGRawDataGroup entries that
    * have the same values. This merge function is used by
    * summaryStats.summarySemigroup to define a combine operation on
    * GHGSummary, which is used to combine records with the same
    * FeatureId in ErrorSummaryRDD. */
  def merge(
    other: GHGSummary
  ): GHGSummary = {
    // the stats.combine method uses the
    // GHGRawData.lossDataSemigroup instance to perform per-value
    // combine on the map.
    GHGSummary(stats.combine(other.stats))
  }

  /** Pivot raw data to GHGData and aggregate across years */
  def toGHGData(): GHGData = {
    if (stats.isEmpty) {
      GHGData.empty
    } else {
      stats
        .map { case (group, data) => group.toGHGData(data.totalArea, data.emissionsCo2e) }
        .foldLeft(GHGData.empty)(_ merge _)
    }
  }

  def isEmpty = stats.isEmpty
}

case class CacheKey(commodity: String, gadmId: String)

object GHGSummary {
  val backupYieldCache = mutable.HashMap[CacheKey, Float]()

  // Cell types of Raster[GHGTile] may not be the same.
  def getGridVisitor(
    kwargs: Map[String, Any]
  ): GridVisitor[Raster[GHGTile],
                 GHGSummary] =
    new GridVisitor[Raster[GHGTile], GHGSummary] {
      private var acc: GHGSummary =
        new GHGSummary()

      def result: GHGSummary = acc

      def visit(raster: Raster[GHGTile],
                col: Int,
                row: Int): Unit = {

        // Look up the "backup" yield based on gadm area (possibly using a cached value).
        def lookupBackupYield(backupArray: Array[Row], commodity: String, gadmId: String): Float = {
          val cached = backupYieldCache.get(CacheKey(commodity, gadmId))
          if (cached.isDefined) {
            return cached.get
          }
          for (r <- backupArray) {
            if (r.getAs[String]("GID_2") == gadmId && r.getAs[String]("commodity") == commodity) {
              val cropYield = r.getAs[String]("yield_kg_ha").toFloat
              backupYieldCache(CacheKey(commodity, gadmId)) = cropYield
              return cropYield
            }
          }
          throw new Exception(s"No yield found for $commodity in $gadmId")
        }

        val featureId = kwargs("featureId").asInstanceOf[GfwProFeatureExtId]

        // This is a pixel by pixel operation

        // pixel Area
        val re: RasterExtent = raster.rasterExtent
        val lat: Double = re.gridRowToMap(row)
        // uses Pixel's center coordinate. +/- raster.cellSize.height/2
        // doesn't make much of a difference
        val area: Double = Geodesy.pixelArea(lat, re.cellSize)
        val areaHa = area / 10000.0

        // input layers
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)

        // Only count tree loss for canopy > 30%
        val umdTreeCoverLossYear: Int = {
          val loss = raster.tile.loss.getData(col, row)
          if (loss != null && tcd2000 > 30) {
            loss.toInt
          } else {
            0
          }
        }

        var cropYield = if (featureId.yieldVal > 0.0) {
          featureId.yieldVal
        } else {
          // Get default yield based on commodity
          featureId.commodity match {
            case "COCO" => raster.tile.cocoYield.getData(col, row)
            case "COFF" => raster.tile.coffYield.getData(col, row)
            case "OILP" => raster.tile.oilpYield.getData(col, row)
            case "RUBB" => raster.tile.rubbYield.getData(col, row)
            case "SOYB" => raster.tile.soybYield.getData(col, row)
            case "SUGC" => raster.tile.sugcYield.getData(col, row)
            case _ => throw new Exception("Invalid commodity")
          }
        }
        println(s"Yield ${cropYield}, (${col}, ${row})")
        if (cropYield == 0) {
          // If we don't have a yield for this commodity based on the specific pixel,
          // then do a lookup for the default yield for the entire gadm2 area this
          // location is in.
          val gadmAdm0: String = raster.tile.gadmAdm0.getData(col, row)
          // Skip processing this pixel if gadmAdm0 is empty
          if (gadmAdm0 == "") {
            return
          }
          val gadmAdm1: Integer = raster.tile.gadmAdm1.getData(col, row)
          val gadmAdm2: Integer = raster.tile.gadmAdm2.getData(col, row)
          val gadmId: String = s"$gadmAdm0.$gadmAdm1.${gadmAdm2}_1"
          println(s"Empty ${featureId.commodity} yield, checking gadm yield $gadmId")
          val backupArray = kwargs("backupYield").asInstanceOf[Broadcast[Array[Row]]].value
          cropYield = lookupBackupYield(backupArray, featureId.commodity, gadmId)
          println(s"Found yield ${cropYield}")
          //val r = backupDF.filter(col("FIPS2") === "AC01001" && col("commodity") == "BANA")
        }

        // Compute gross emissions Co2-equivalent due to tree loss at this pixel.
        val grossEmissionsCo2eNonCo2: Float = raster.tile.grossEmissionsCo2eNonCo2.getData(col, row)
        val grossEmissionsCo2eCo2Only: Float =  raster.tile.grossEmissionsCo2eCo2Only.getData(col, row)
        val grossEmissionsCo2eNonCo2Pixel = grossEmissionsCo2eNonCo2 * areaHa
        val grossEmissionsCo2eCo2OnlyPixel = grossEmissionsCo2eCo2Only * areaHa

        val groupKey = GHGRawDataGroup(
          umdTreeCoverLossYear,
          cropYield
        )

        // if (umdTreeCoverLossYear > 0) {
        //   println(s"Yield $cropYield, lossYear $umdTreeCoverLossYear, area $areaHa, co2e ${grossEmissionsCo2eNonCo2Pixel + grossEmissionsCo2eCo2OnlyPixel}")
        // }
        val summaryData: GHGRawData =
          acc.stats.getOrElse(
            key = groupKey,
            default = GHGRawData(0, 0)
          )

        summaryData.totalArea += areaHa
        summaryData.emissionsCo2e += grossEmissionsCo2eNonCo2Pixel + grossEmissionsCo2eCo2OnlyPixel

        val new_stats = acc.stats.updated(groupKey, summaryData)
        acc = GHGSummary(new_stats)

      }
    }
}
