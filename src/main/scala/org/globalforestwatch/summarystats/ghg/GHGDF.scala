package org.globalforestwatch.summarystats.ghg

import org.apache.spark.sql.functions.round
import cats.data.Validated.{Invalid, Valid}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats.{ValidatedLocation, Location}
import org.globalforestwatch.util.Util.fieldsFromCol
import org.globalforestwatch.summarystats.SummaryDF
import org.globalforestwatch.summarystats.SummaryDF.{RowError, RowId}

object GHGDF extends SummaryDF {

  def getFeatureDataFrame(
    dataRDD: RDD[ValidatedLocation[GHGData]],
    spark: SparkSession
  ): DataFrame = {
    import spark.implicits._

    val rowId: FeatureId => RowId = {
      case gfwproId: GfwProFeatureExtId =>
        RowId(gfwproId.listId, gfwproId.locationId.toString)
      case id =>
        throw new IllegalArgumentException(s"Can't produce DataFrame for $id")
    }

    dataRDD.map {
      case Valid(Location(fid, data)) =>
        (rowId(fid), RowError.empty, data)
      case Invalid(Location(fid, err)) =>
        (rowId(fid), RowError.fromJobError(err), GHGData.empty)
    }
      .toDF("id", "error", "data")
      .select($"id.*" :: $"error.*" :: fieldsFromCol($"data", featureFields): _*)
      .withColumn("weighted_avg_yield", round($"production" * 1000 / $"total_area", 4))
  }

  val featureFields = List(
    "total_area",
    "ef_co2_yearly",
    "ef_ch4_yearly",
    "ef_n2o_yearly",
    "emissions_factor_yearly",
    "emissions_co2_yearly",
    "emissions_ch4_yearly",
    "emissions_n2o_yearly",
    "emissions_yearly",
    "production",
  )
}
