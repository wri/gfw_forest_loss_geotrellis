package org.globalforestwatch.summarystats.afi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{CombinedFeatureId, FeatureId, GadmFeatureId, GfwProFeatureId}
import org.globalforestwatch.summarystats._
import cats.data.Validated.{Invalid, Valid}
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticData

object AFiDF extends SummaryDF {
  case class RowGadmId(list_id: String, location_id: String, gadm_id: String)

  def getFeatureDataFrame(
    dataRDD: RDD[ValidatedLocation[AFiData]],
    spark: SparkSession
  ): DataFrame = {
    import spark.implicits._

    dataRDD.mapValues {
      case Valid(data) =>
        (SummaryDF.RowError.empty, data)
      case Invalid(err) =>
        (SummaryDF.RowError.fromJobError(err), AFiData.empty)
    }.map {
      case (CombinedFeatureId(proId: GfwProFeatureId, gadmId: GadmFeatureId), (error, data)) =>
        val rowId = RowGadmId(proId.listId, proId.locationId.toString, gadmId.toString())
        (rowId, error, data)
      case _ =>
        throw new IllegalArgumentException("Not a CombinedFeatureId[GfwProFeatureId, GadmFeatureId]")
    }
    .toDF("id", "error", "data")
    .select($"id.*", $"error.*", $"data.*")
  }
}
