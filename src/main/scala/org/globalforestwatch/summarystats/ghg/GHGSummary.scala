package org.globalforestwatch.summarystats.ghg

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.summary.GridVisitor
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy
import org.apache.spark.broadcast.Broadcast
import org.globalforestwatch.features.GfwProFeatureExtId
import scala.collection.mutable
import org.globalforestwatch.summarystats.NoYieldException

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

  /** Pivot raw data to GHGData and aggregate across groups */
  def toGHGData(): GHGData = {
    if (stats.isEmpty) {
      GHGData.empty
    } else {
      stats
        .map { case (group, data) => group.toGHGData(data.totalArea, data.emissionsCo2eCO2, data.emissionsCo2eCH4, data.emissionsCo2eN2O, data.emissionsCo2e) }
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
        def lookupBackupYield(backupYieldArray: Array[Array[String]], commodity: String, gadmId: String): Float = {
          val cached = backupYieldCache.get(CacheKey(commodity, gadmId))
          if (cached.isDefined) {
            return cached.get
          }
          val header = backupYieldArray.head
          val gid2Index = header.indexOf("GID_2")
          val commodityIndex = header.indexOf("commodity")
          val yieldIndex = header.indexOf("yield_kg_ha")
          for (r <- backupYieldArray) {
            if (r(gid2Index) == gadmId && r(commodityIndex) == commodity) {
              val cropYield = r(yieldIndex).toFloat
              backupYieldCache(CacheKey(commodity, gadmId)) = cropYield
              println(s"Found backupyield ${cropYield} for ${commodity} in ${gadmId}")
              return cropYield
            }
          }
          println(s"No yield found for $commodity in $gadmId")
          throw new NoYieldException(s"No yield found for $commodity in $gadmId")
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

        // Only count tree loss for canopy > 10% OR if there was some tree gain OR
        // if there are mangroves, but always ignore it if plantationsPre2000 is true.
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val treeGainFromHeight: Boolean = raster.tile.treeCoverGainFromHeight.getData(col, row)
        val mangroveBiomassExtent: Boolean = raster.tile.mangroveBiomassExtent.getData(col, row)
        val umdTreeCoverLossYear: Int = {
          val plantationsPre2000 = raster.tile.plantationsPre2000.getData(col, row)
          if (plantationsPre2000) {
            0
          } else {
            val loss = raster.tile.loss.getData(col, row)
            if (loss != null && (tcd2000 > 10 || treeGainFromHeight || mangroveBiomassExtent)) {
              loss.toInt
            } else {
              0
            }
          }
        }

        val cropYield = if (featureId.yieldVal > 0.0) {
          featureId.yieldVal
        } else {
          // Get default yield based on commodity
          val defaultYield = featureId.commodity match {
            case "COCO" => raster.tile.cocoYield.getData(col, row)
            case "COFF" => raster.tile.coffYield.getData(col, row)
            case "OILP" => raster.tile.oilpYield.getData(col, row)
            case "RUBB" => raster.tile.rubbYield.getData(col, row)
            case "SOYB" => raster.tile.soybYield.getData(col, row)
            case "SUGC" => raster.tile.sugcYield.getData(col, row)
            case _ =>
              println(s"Invalid commodity ${featureId.commodity}")
              throw new Exception("Invalid commodity")
          }
          if (defaultYield != 0.0) {
            //println(s"MapSpam Yield ${defaultYield}, (${col}, ${row}), featureId ${featureId}")
            defaultYield
          } else {
            // If we don't have a yield for this commodity based on the specific pixel,
            // then do a lookup for the default yield for the entire gadm2 area this
            // location is in.
            val gadmAdm0: String = raster.tile.gadmAdm0.getData(col, row)
            // Skip processing this pixel if gadmAdm0 is empty
            if (gadmAdm0 == "") {
              println(s"Empty gadmAdm0, $featureId, ($col, $row)")
              return
            }
            val gadmAdm1: Integer = raster.tile.gadmAdm1.getData(col, row)
            val gadmAdm2: Integer = raster.tile.gadmAdm2.getData(col, row)
            val gadmId: String = s"$gadmAdm0.$gadmAdm1.${gadmAdm2}_1"
            val backupYieldArray = kwargs("backupYield").asInstanceOf[Broadcast[Array[Array[String]]]].value
            val backupYield = lookupBackupYield(backupYieldArray, featureId.commodity, gadmId)
            backupYield
          }
        }

        // Compute gross emissions Co2-equivalent due to tree loss at this pixel.
        val grossEmissionsCo2eCo2Only: Float = raster.tile.grossEmissionsCo2eCo2Only.getData(col, row)
        val grossEmissionsCo2eCH4: Float =  raster.tile.grossEmissionsCo2eCH4.getData(col, row)
        val grossEmissionsCo2eN2O: Float =  raster.tile.grossEmissionsCo2eN2O.getData(col, row)
        val grossEmissionsCo2eCo2OnlyPixel = grossEmissionsCo2eCo2Only * areaHa
        val grossEmissionsCo2eCH4Pixel = grossEmissionsCo2eCH4 * areaHa
        val grossEmissionsCo2eN2OPixel = grossEmissionsCo2eN2O * areaHa

        //if (raster.tile.loss.getData(col, row) != null && !(tcd2000 > 10 || treeGainFromHeight || mangroveBiomassExtent)) {
        //  println(s"Pixel with TCL ${raster.tile.loss.getData(col, row).toInt} but tcd2000 $tcd2000, $treeGainFromHeight, $mangroveBiomassExtent, emissions ${grossEmissionsCo2eCo2OnlyPixel + grossEmissionsCo2eCH4Pixel + grossEmissionsCo2eN2OPixel}")
        //}

        val groupKey = GHGRawDataGroup(umdTreeCoverLossYear, cropYield)

        val summaryData: GHGRawData =
          acc.stats.getOrElse(
            key = groupKey,
            default = GHGRawData(0, 0, 0, 0, 0)
          )

        summaryData.totalArea += areaHa
        summaryData.emissionsCo2eCO2 += grossEmissionsCo2eCo2OnlyPixel
        summaryData.emissionsCo2eCH4 += grossEmissionsCo2eCH4Pixel
        summaryData.emissionsCo2eN2O += grossEmissionsCo2eN2OPixel
        summaryData.emissionsCo2e += grossEmissionsCo2eCo2OnlyPixel + grossEmissionsCo2eCH4Pixel + grossEmissionsCo2eN2OPixel
        //println(s"FeatureId ${featureId.listId}, ${featureId.locationId}, TCL ${umdTreeCoverLossYear}, yield ${cropYield}, emiss of pixel ${summaryData.emissionsCo2e}, pixel area: ${areaHa}, (${col}, ${row})")

        val new_stats = acc.stats.updated(groupKey, summaryData)
        acc = GHGSummary(new_stats)

      }
    }
}
