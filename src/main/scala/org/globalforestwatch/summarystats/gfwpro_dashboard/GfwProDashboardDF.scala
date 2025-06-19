package org.globalforestwatch.summarystats.gfwpro_dashboard

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, GfwProFeatureId, CombinedFeatureId}
import org.globalforestwatch.summarystats._
import cats.data.Validated.{Valid, Invalid}
import org.apache.spark.sql.functions.expr
import org.globalforestwatch.summarystats.SummaryDF.RowId
import cats.kernel.Semigroup

object GfwProDashboardDF extends SummaryDF {

  def getFeatureDataFrameFromVerifiedRdd(
    dataRDD: RDD[ValidatedLocation[GfwProDashboardData]],
    doGadmIntersect: Boolean,
    spark: SparkSession
  ): DataFrame = {
    import spark.implicits._

    def combineRows(v1: Object, v2: Object): Object = {
      if (v1.isInstanceOf[GfwProDashboardData]) {
        if (v2.isInstanceOf[GfwProDashboardData]) {
          v1.asInstanceOf[GfwProDashboardData].merge(v2.asInstanceOf[GfwProDashboardData])
        } else {
          v2
        }
      } else {
        v1
      }
    }

    val reducedRDD = if (!doGadmIntersect) {
      dataRDD
    } else {
      // In case of vector intersection, we need to do a final combine of rows with
      // the same featureId, to handle the case where a feature geometry had to be
      // split, because one of the split rows will have had the correct gadm2 id in
      // its feature id, whereas the gadm2 id will be blank in the other split rows.
      dataRDD.map {
        case Valid(Location(id, data)) =>
          (id, data)
        case Invalid(Location(id, err)) =>
          (id, err)
      }.reduceByKey(combineRows).map {
        case (id: FeatureId, data: GfwProDashboardData) => Valid(Location(id, data))
        case (id: FeatureId, err: JobError) => Invalid(Location(id, err))
        case _ => throw new IllegalArgumentException("Missing case")
      }
    }

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
    reducedRDD.map {
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
