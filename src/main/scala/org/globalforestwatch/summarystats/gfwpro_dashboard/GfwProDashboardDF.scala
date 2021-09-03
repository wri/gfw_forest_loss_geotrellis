package org.globalforestwatch.summarystats.gfwpro_dashboard

import io.circe.syntax._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{CombinedFeatureId, FeatureId, GadmFeatureId, GfwProFeatureId}
import org.globalforestwatch.summarystats.FramelessEncoders

object GfwProDashboardDF extends FramelessEncoders {
  case class RowId(list_id: String, location_id: String, gadm_id: String)

  def getFeatureDataFrame(
    dataRDD: RDD[(FeatureId, GfwProDashboardData)],
    spark: SparkSession
  ): DataFrame = {
    import spark.implicits._

    dataRDD.map {
      case (CombinedFeatureId(proId: GfwProFeatureId, gadmId: GadmFeatureId), data) =>
        val rowId = RowId(proId.listId, proId.locationId.toString, gadmId.toString())
        (rowId, data)
      case _ =>
        throw new IllegalArgumentException("Not a CombinedFeatureId[GfwProFeatureId, GadmFeatureId]")
    }
      .toDF("id", "data")
      .select($"id.*", $"data.*")
  }
}
