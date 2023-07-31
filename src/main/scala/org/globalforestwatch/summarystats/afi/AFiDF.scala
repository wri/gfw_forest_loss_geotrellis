package org.globalforestwatch.summarystats.afi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{CombinedFeatureId, FeatureId, GadmFeatureId, GfwProFeatureId, WdpaFeatureId}
import org.globalforestwatch.summarystats._
import cats.data.Validated.{Invalid, Valid}
import org.globalforestwatch.summarystats.SummaryDF.{RowError, RowId}
import org.globalforestwatch.util.Util.fieldsFromCol

object AFiDF extends SummaryDF {
  case class RowGadmId(list_id: String, location_id: String, gadm_id: String)

  def getFeatureDataFrame(
    dataRDD: RDD[ValidatedLocation[AFiData]],
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

    dataRDD
      .map {
        case Valid(Location(fid, data)) =>
          (rowId(fid), RowError.empty, data)
        case Invalid(Location(fid, err)) =>
          (rowId(fid), RowError.fromJobError(err), AFiData.empty)
      }
      .toDF("id", "error", "data")
      .select($"id.*", $"error.*", $"data.*")
  }
}
