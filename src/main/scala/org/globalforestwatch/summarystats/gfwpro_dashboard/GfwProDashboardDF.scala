package org.globalforestwatch.summarystats.gfwpro_dashboard

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{CombinedFeatureId, FeatureId, GadmFeatureId, GfwProFeatureId}
import org.globalforestwatch.summarystats._
import cats.data.Validated.{Valid, Invalid}
import org.apache.spark.sql.functions.expr
import org.globalforestwatch.summarystats.SummaryDF.RowId

object GfwProDashboardDF extends SummaryDF {
  case class RowGadmId(list_id: String, location_id: String, gadm_id: String)

  def getFeatureDataFrameFromVerifiedRdd(
    dataRDD: RDD[ValidatedLocation[GfwProDashboardData]],
    spark: SparkSession
  ): DataFrame = {
    import spark.implicits._

    val rowId: FeatureId => RowId = {
      case proId: GfwProFeatureId =>
        RowId(proId.listId, proId.locationId.toString)
      case _ =>
        throw new IllegalArgumentException("Not a CombinedFeatureId[GfwProFeatureId, GadmFeatureId]")
    }
    dataRDD.map {
      case Valid(Location(id, data)) =>
        (rowId(id), SummaryDF.RowError.empty, data)
      case Invalid(Location(id, err)) =>
        (rowId(id), SummaryDF.RowError.fromJobError(err), GfwProDashboardData.empty)
    }
    .toDF("id", "error", "data")
    .select($"id.*", expr("data.mygadm as gadm_id"), $"error.*", $"data.*")
    .drop($"mygadm")
  }

  def getFeatureDataFrame(
    dataRDD: RDD[(FeatureId, ValidatedRow[GfwProDashboardData])],
    spark: SparkSession
  ): DataFrame = {
    import spark.implicits._

    dataRDD.mapValues {
      case Valid(data) =>
        (SummaryDF.RowError.empty, data)
      case Invalid(err) =>
        (SummaryDF.RowError.fromJobError(err), GfwProDashboardData.empty)
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
