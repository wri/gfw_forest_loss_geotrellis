package org.globalforestwatch.summarystats.gfwpro_dashboard

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, GfwProFeatureId, CombinedFeatureId}
import org.globalforestwatch.summarystats._
import cats.data.Validated.{Valid, Invalid}
import org.apache.spark.sql.functions.expr
import org.globalforestwatch.summarystats.SummaryDF.RowId

object GfwProDashboardDF extends SummaryDF {

  def getFeatureDataFrameFromVerifiedRdd(
    dataRDD: RDD[ValidatedLocation[GfwProDashboardData]],
    spark: SparkSession
  ): DataFrame = {
    import spark.implicits._

    val rowId: FeatureId => RowId = {
      case proId: GfwProFeatureId =>
        RowId(proId.listId, proId.locationId.toString)
      // The following case is to deal with Invalid rows, which have not had the gadm
      // information moved out of the feature id.
      case CombinedFeatureId(proId: GfwProFeatureId, _) =>
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
    // Put data.group_gadm_id right after list/location and rename to gadm_id
    .select($"id.*", expr("data.group_gadm_id as gadm_id"), $"error.*", $"data.*")
    .drop($"group_gadm_id")
  }
}
