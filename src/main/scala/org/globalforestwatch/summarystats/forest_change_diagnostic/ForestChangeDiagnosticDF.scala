package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats.{JobError, ValidatedLocation, Location}
import org.globalforestwatch.util.Util.{colsFor, fieldsFromCol}
import org.globalforestwatch.summarystats.SummaryDF
import org.globalforestwatch.summarystats.SummaryDF.{RowError, RowId}

object ForestChangeDiagnosticDF extends SummaryDF {

  def getFeatureDataFrame(
    dataRDD: RDD[ValidatedLocation[ForestChangeDiagnosticData]],
    spark: SparkSession
  ): DataFrame = {
    import spark.implicits._

    val rowId: FeatureId => RowId = {
      case gfwproId: GfwProFeatureId =>
        RowId(gfwproId.listId, gfwproId.locationId.toString)
      case gadmId: GadmFeatureId =>
        RowId("GADM 3.6", gadmId.toString)
      case wdpaId: WdpaFeatureId =>
        RowId("WDPA", wdpaId.toString)
      case id =>
        throw new IllegalArgumentException(s"Can't produce DataFrame for $id")
    }

    dataRDD.map {
      case Valid(Location(fid, data)) =>
        (rowId(fid), RowError.empty, data)
      case Invalid(Location(fid, err)) =>
        (rowId(fid), RowError.fromJobError(err), ForestChangeDiagnosticData.empty)
    }
      .toDF("id", "error", "data")
      .select($"id.*" :: $"error.*" :: fieldsFromCol($"data", featureFields): _*)
  }

  def getGridFeatureDataFrame(
    dataRDD: RDD[ValidatedLocation[ForestChangeDiagnosticData]],
    spark: SparkSession
  ): DataFrame = {
    import spark.implicits._

    val rowId: FeatureId => RowGridId = {
      case CombinedFeatureId(gfwproId: GfwProFeatureId, gridId: GridId) =>
        RowGridId(gfwproId, gridId)
      case _ =>
        throw new IllegalArgumentException("Not a CombinedFeatureId")
    }

    dataRDD.map {
      case Valid(Location(fid, data)) =>
        (rowId(fid), RowError.empty, data)
      case Invalid(Location(fid, err)) =>
        (rowId(fid), RowError.fromJobError(err), ForestChangeDiagnosticData.empty)
    }
      .toDF("id", "error", "data")
      .select($"id.*" :: $"error.*" :: fieldsFromCol($"data", featureFields) ::: fieldsFromCol($"data", gridFields): _*)
  }

  def readIntermidateRDD(
    sources: NonEmptyList[String],
    spark: SparkSession,
  ): RDD[ValidatedLocation[ForestChangeDiagnosticData]] = {
    val df = FeatureDF(sources, GfwProFeature, FeatureFilter.empty, spark)
    val ds = df.select(
      colsFor[RowGridId].as[RowGridId],
      colsFor[RowError].as[RowError],
      colsFor[ForestChangeDiagnosticData].as[ForestChangeDiagnosticData])

    ds.rdd.map { case (id, error, data) =>
      if (error.status_code == 2) Valid(Location(id.toFeatureID, data))
      else Invalid(Location(id.toFeatureID, JobError.fromErrorColumn(error.location_error).get))
    }
  }

  case class RowGridId(list_id: String, location_id: Int, x: Double, y: Double, grid: String) {
    def toFeatureID = CombinedFeatureId(GfwProFeatureId(list_id, location_id , x, y), GridId(grid))
  }

  object RowGridId {
    def apply(gfwProId: GfwProFeatureId, gridId: GridId): RowGridId = RowGridId (
      list_id = gfwProId.listId,
      location_id = gfwProId.locationId,
      x = gfwProId.x,
      y = gfwProId.y,
      grid = gridId.gridId)
  }

  val featureFields = List(
    "tree_cover_loss_total_yearly", // treeCoverLossYearly
    "tree_cover_loss_primary_forest_yearly", // treeCoverLossPrimaryForestYearly
    "tree_cover_loss_peat_yearly", //treeCoverLossPeatLandYearly
    "tree_cover_loss_intact_forest_yearly", // treeCoverLossIntactForestYearly
    "tree_cover_loss_protected_areas_yearly", // treeCoverLossProtectedAreasYearly
    "tree_cover_loss_arg_otbn_yearly", // treeCoverLossARGOTBNYearly
    "tree_cover_loss_sea_landcover_yearly", // treeCoverLossSEAsiaLandCoverYearly
    "tree_cover_loss_idn_landcover_yearly", // treeCoverLossIDNLandCoverYearly
    "tree_cover_loss_soy_yearly", // treeCoverLossSoyPlanedAreasYearly
    "tree_cover_loss_idn_legal_yearly", // treeCoverLossIDNForestAreaYearly
    "tree_cover_loss_idn_forest_moratorium_yearly", // treeCoverLossIDNForestMoratoriumYearly
    "tree_cover_loss_prodes_yearly", // prodesLossYearly
    "tree_cover_loss_prodes_wdpa_yearly", // prodesLossProtectedAreasYearly
    "tree_cover_loss_prodes_primary_forest_yearly", // prodesLossProdesPrimaryForestYearly
    "tree_cover_loss_brazil_biomes_yearly", // treeCoverLossBRABiomesYearly
    "tree_cover_extent_total", // treeCoverExtent
    "tree_cover_extent_primary_forest", // treeCoverExtentPrimaryForest
    "tree_cover_extent_protected_areas", // treeCoverExtentProtectedAreas
    "tree_cover_extent_peat", // treeCoverExtentPeatlands
    "tree_cover_extent_intact_forest", // treeCoverExtentIntactForests
    "natural_habitat_primary", // primaryForestArea
    "natural_habitat_intact_forest", //intactForest2016Area
    "total_area", // totalArea
    "protected_areas_area", // protectedAreasArea
    "peat_area", // peatlandsArea
    "arg_otbn_area", // argOTBNArea
    "brazil_biomes", // braBiomesArea
    "idn_legal_area", // idnForestAreaArea
    "sea_landcover_area", // seAsiaLandCoverArea
    "idn_landcover_area", // idnLandCoverArea
    "idn_forest_moratorium_area", // idnForestMoratoriumArea
    "south_america_presence", // southAmericaPresence,
    "legal_amazon_presence", // legalAmazonPresence,
    "brazil_biomes_presence", // braBiomesPresence,
    "cerrado_biome_presence", // cerradoBiomesPresence,
    "southeast_asia_presence", // seAsiaPresence,
    "indonesia_presence", // idnPresence
    "argentina_presence", // argPresence
    "commodity_value_forest_extent", //  forestValueIndicator
    "commodity_value_peat", // peatValueIndicator
    "commodity_value_protected_areas", // protectedAreaValueIndicator
    "commodity_threat_deforestation", // deforestationThreatIndicator
    "commodity_threat_peat", // peatThreatIndicator
    "commodity_threat_protected_areas", // protectedAreaThreatIndicator
    "commodity_threat_fires" // fireThreatIndicator
  )

  val gridFields = List(
    "tree_cover_loss_tcd90_yearly", // treeCoverLossTcd90Yearly
    "filtered_tree_cover_extent", // filteredTreeCoverExtent
    "filtered_tree_cover_extent_yearly", //filteredTreeCoverExtentYearly
    "filtered_tree_cover_loss_yearly", //filteredTreeCoverLossYearly
    "filtered_tree_cover_loss_peat_yearly", //filteredTreeCoverLossPeatYearly
    "filtered_tree_cover_loss_protected_areas_yearly", // filteredTreeCoverLossProtectedAreasYearly
    "plantation_area", // plantationArea
    "plantation_on_peat_area", // plantationOnPeatArea
    "plantation_in_protected_areas_area" //plantationInProtectedAreasArea
  )

}