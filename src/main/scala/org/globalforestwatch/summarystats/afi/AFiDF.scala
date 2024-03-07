package org.globalforestwatch.summarystats.afi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, GadmFeatureId, GfwProFeatureId, WdpaFeatureId}
import org.globalforestwatch.summarystats._
import cats.data.Validated.{Invalid, Valid}
import org.globalforestwatch.summarystats.SummaryDF.{RowError, RowId}
import org.apache.spark.sql.functions.col

object AFiDF extends SummaryDF {
  case class RowGadmId(list_id: String, location_id: String, gadm_id: String)
  case class AFiStatusAndData(status_code: Int, location_error: String, data: AFiData)

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

    val baseRDD = summaryRDD
      .flatMap {
        case Valid(Location(fid, data)) =>
          data.stats.map {
            case (dataGroup, data) => {
              val r = rowId(fid)
              val e = RowError.empty
              // Change gadm_id to "" for all non-dissolved rows.
              val gadm_id = if (r.location_id == "-1") {
                dataGroup.gadm_id
              } else {
                ""
              }
              val location_error = if (e.location_error == null) "" else e.location_error
              (RowGadmId(r.list_id, r.location_id, gadm_id),
                AFiStatusAndData(e.status_code, location_error, data))
            }
          }
        case Invalid(Location(fid, err)) => {
          val r = rowId(fid)
          val e = RowError.fromJobError(err)
          val d = AFiDataGroup.empty
          List((RowGadmId(r.list_id, r.location_id, ""), AFiStatusAndData(e.status_code, e.location_error, AFiData.empty)))
        }
      }

    // Aggregate all results for each unique (list_id, location_id, gadm_id). Since
    // gadm_id has already been set to "" for all rows with location_id != -1, all
    // results for each unique (list_id, location_id) for location_id != -1 are
    // combined.
    val reducedRDD = baseRDD.reduceByKey(reduceOp _)
    //reduceRDD.collect().foreach(println)

    // Aggregate results for all (list_id, -1) (ignoring gadm_id), and create summary
    // row (list_id, -1, "").
    val aggRDD = reducedRDD.filter(row => row._1.location_id == "-1").map(row => (AFiDF.RowGadmId(row._1.list_id, row._1.location_id, ""), row._2)).reduceByKey(reduceOp _)
    //aggRDD.collect().foreach(println)

    // Add the summary rows (list_id, -1, "") to the reduced rdd.
    val finalRDD = reducedRDD.union(aggRDD)
    //finalRDD.collect().foreach(println)

    val combinedDF = finalRDD.toDF("key", "value").select(col("key.*"), col("value.*")).select(col("list_id"), col("location_id"), col("gadm_id"),  col("data.*"), col("status_code"), col("location_error"))
    combinedDF.show(100, truncate=false)
    combinedDF
  }

  // Function to combine rows of (RowGadmId, AFiStatusAndData) grouped by RowGadmId key.
  def reduceOp(a: AFiStatusAndData, b: AFiStatusAndData): AFiStatusAndData = {
    val status_code = if (a.status_code >= b.status_code)
      a.status_code
    else
      b.status_code
    val location_error = if (a.location_error != "") {
      if (b.location_error != "")
        a.location_error + ", " + b.location_error
      else
        a.location_error
    } else if (b.location_error != "")
      b.location_error
    else
      ""
    AFiStatusAndData(status_code, location_error, a.data.merge(b.data))
  }
}
