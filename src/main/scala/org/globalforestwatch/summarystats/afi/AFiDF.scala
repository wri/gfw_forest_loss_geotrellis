package org.globalforestwatch.summarystats.afi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, GadmFeatureId, GfwProFeatureId, WdpaFeatureId}
import org.globalforestwatch.summarystats._
import cats.data.Validated.{Invalid, Valid}
import org.globalforestwatch.summarystats.SummaryDF.{RowError, RowId}

object AFiDF extends SummaryDF {
  case class RowGadmId(list_id: String, location_id: String, gadm_id: String)

  def getFeatureDataFrame(
    summaryRDD: RDD[ValidatedLocation[AFiSummary]],
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

    summaryRDD
      .flatMap {
        case Valid(Location(fid, data)) =>
          data.stats.map {
            case (dataGroup, data) =>
              (rowId(fid), RowError.empty, dataGroup, data)
          }
        case Invalid(Location(fid, err)) =>
         List((rowId(fid), RowError.fromJobError(err), AFiDataGroup.empty, AFiData.empty))
      }
      .toDF("id", "error", "dataGroup", "data")
      .select($"id.*", $"error.*", $"dataGroup.*", $"data.*")
  }
}
