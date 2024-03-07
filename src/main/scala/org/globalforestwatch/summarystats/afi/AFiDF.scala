package org.globalforestwatch.summarystats.afi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.{FeatureId, GadmFeatureId, GfwProFeatureId, WdpaFeatureId}
import org.globalforestwatch.summarystats._
import cats.data.Validated.{Invalid, Valid}
import org.globalforestwatch.summarystats.SummaryDF.{RowError, RowId}

object AFiDF extends SummaryDF {
  case class RowGadmId(list_id: String, location_id: String, gadm_id: String)

  def getFeatureDataFrame(
    summaryRDD: RDD[ValidatedLocation[AFiSummary]],
    spark: SparkSession
  ): RDD[(RowGadmId, (Int, String, AFiData))] = {

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
            case (dataGroup, data) => {
              val r = rowId(fid)
              val e = RowError.empty
              val gadm_id = if (r.location_id == "-1") {
                dataGroup.gadm_id
              } else {
                ""
              }
              (RowGadmId(r.list_id, r.location_id, gadm_id),
                (e.status_code, e.location_error, data))
            }
          }
        case Invalid(Location(fid, err)) => {
          val r = rowId(fid)
          val e = RowError.empty
          val d = AFiDataGroup.empty
          List((RowGadmId(r.list_id, r.location_id, ""), (e.status_code, e.location_error, AFiData.empty)))
        }
      }
  }
}
