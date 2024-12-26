package org.globalforestwatch.summarystats.afi
import org.apache.spark.sql.functions.{col, lit, when, sum, max, concat_ws, collect_list}
import cats.data.Validated
import geotrellis.vector.{Feature, Geometry}
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import scala.collection.immutable.SortedMap
import io.circe.syntax._
import cats.data.Validated.{Invalid, Valid}

object AFiAnalysis extends SummaryAnalysis {

  val name = "afi"

  // UDF to convert unsorted map with some year-loss entries to SortedMap with all
  // year entries (even if forest loss was zero), and then convert to JSON string.
  val toSortedMapUDF = udf((map: Map[Int, Double]) => {
    val yearList = (for (i <- AFiCommand.TreeCoverLossYearStart to AFiCommand.TreeCoverLossYearEnd) yield(i -> 0.0))
    val populatedMap = SortedMap[Int, Double](yearList: _*) ++ map
    populatedMap.asJson.noSpaces
  })
  
  def apply(
    featureRDD: RDD[ValidatedLocation[Geometry]],
    featureType: String,
    spark: SparkSession,
    kwargs: Map[String, Any]
  ): DataFrame = {
    featureRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // TODO invalid should map to job error somehow, probably using ValidatedWorkflow
    val validatedRDD = featureRDD.map {
      case Validated.Valid(Location(id, geom: Geometry))   => Feature(geom, id)
      case Validated.Invalid(Location(id, geom: Geometry)) => Feature(geom, id)
    }

    val summaryRDD: RDD[ValidatedLocation[AFiSummary]] = AFiRDD(validatedRDD, AFiGrid.blockTileGrid, kwargs)

    import spark.implicits._

    // If a location has no AFiDataGroup entries, then the geometry must not have
    // intersected the centroid of any pixels, so report the location as
    // NoIntersectionError.
     val summary1RDD = summaryRDD.map {
       case Valid(Location(fid, data)) if data.isEmpty =>
         Invalid(Location(fid, NoIntersectionError))
       case data => data
     }

    // Null out gadm_id for all non-dissolved rows and then aggregate all results for
    // each unique (list_id, location_id, gadm_id, loss_year). Need to combine first
    // with key including loss_year, so we don't have duplicate loss year entries
    // when we create the map of loss years.
    val summary1DF = AFiAnalysis.aggregateByLossYear(
        AFiDF
        .getFeatureDataFrame(summary1RDD, spark)
        .withColumn(
          "gadm_id", when(col("location_id") =!= -1, lit("") ).otherwise(col("gadm_id"))
        )
        .groupBy($"list_id", $"location_id", $"gadm_id", $"loss_year")
    )
    
    // Now aggregate where we combine results for different loss_years, creating new
    // columns with per-year forest loss.
    val summaryDF= AFiAnalysis.aggregateResults(summary1DF
      .groupBy($"list_id", $"location_id", $"gadm_id"))

    // For each unique list_id, aggregate all dissolved rows with that list_id, and
    // create a summary row (list_id, -1, ""). Must first aggregate with key
    // including loss_year, to make sure there are no duplicate loss years from
    // merging rows with different GADMs.
    val gadmAgg1 = AFiAnalysis.aggregateByLossYear(
      summary1DF
      .filter($"location_id" === -1)
      .withColumn("gadm_id", lit(""))
      .groupBy($"list_id", $"location_id", $"gadm_id", $"loss_year")
    )
    val gadmAgg = AFiAnalysis.aggregateResults(
      gadmAgg1
      .groupBy($"list_id"),
    )
      .withColumn("gadm_id", lit(""))
      .withColumn("location_id", lit(-1))

    // Add in the summary rows.
    val combinedDF = summaryDF.unionByName(gadmAgg)

    // Replace negligible_risk_area__ha by neglible_risk__percent. Change
    // natural_forest_loss_by_year__ha and jrc_forest_loss_by_year__ha to sorted maps
    // with all year entries, then converted to json strings.
    val resultsDF = combinedDF
      .withColumn(
        "negligible_risk__percent",
        $"negligible_risk_area__ha" / $"total_area__ha" * 100
      )
      .drop("negligible_risk_area__ha")
      .withColumn("natural_forest_loss_by_year__ha", toSortedMapUDF(col("natural_forest_loss_by_year__ha")))
      .withColumn("jrc_forest_loss_by_year__ha", toSortedMapUDF(col("jrc_forest_loss_by_year__ha")))

    resultsDF
      .withColumn("list_id", col("list_id").cast(IntegerType))
      .withColumn("location_id", col("location_id").cast(IntegerType))
      .withColumn("natural_forest__extent", round($"natural_forest__extent", 4))
      .withColumn("natural_forest_loss__ha", round($"natural_forest_loss__ha", 4))
      .withColumn("jrc_forest_cover__extent", round($"jrc_forest_cover__extent", 4))
      .withColumn("jrc_forest_cover_loss__ha", round($"jrc_forest_cover_loss__ha", 4))
      .withColumn("protected_areas_area__ha", round($"protected_areas_area__ha", 4))
      .withColumn("landmark_area__ha", round($"landmark_area__ha", 4))
      .withColumn("total_area__ha", round($"total_area__ha", 4))
      .withColumn("negligible_risk__percent", round($"negligible_risk__percent", 4))
  }

  // Aggregate all entries with same (list_id, location_id, gadm_id, loss_year)
  private def aggregateByLossYear(group: RelationalGroupedDataset) = {
    group.agg(
        sum("natural_forest__extent").alias("natural_forest__extent"),
        sum("jrc_forest_cover__extent").alias("jrc_forest_cover__extent"),
        sum("negligible_risk_area__ha").alias("negligible_risk_area__ha"),
        sum("protected_areas_area__ha").alias("protected_areas_area__ha"),
        sum("landmark_area__ha").alias("landmark_area__ha"),
        sum("total_area__ha").alias("total_area__ha"),
        max("status_code").alias("status_code"),
        concat_ws(", ", collect_list(when(col("location_error").isNotNull && col("location_error") =!= "", col("location_error")))).alias("location_error")
      )
  }

  // Aggregate all entries with same (list_id, location_id, gadm_id), combining
  // different loss_year entries in JSON objects showing loss per year.
  private def aggregateResults(group: RelationalGroupedDataset) = {
    group.agg(
        sum("natural_forest__extent").alias("natural_forest__extent"),
        sum(when(col("loss_year") =!= 0, col("natural_forest__extent")).otherwise(0.0)).alias("natural_forest_loss__ha"),
        map_from_arrays(collect_list(when(col("loss_year") =!= 0, col("loss_year"))), collect_list(when(col("loss_year") =!= 0, round(col("natural_forest__extent"), 4)))).alias("natural_forest_loss_by_year__ha"),
        sum("jrc_forest_cover__extent").alias("jrc_forest_cover__extent"),
        sum(when(col("loss_year") =!= 0, col("jrc_forest_cover__extent")).otherwise(0.0)).alias("jrc_forest_cover_loss__ha"),
        map_from_arrays(collect_list(when(col("loss_year") =!= 0, col("loss_year"))), collect_list(when(col("loss_year") =!= 0, round(col("jrc_forest_cover__extent"), 4)))).alias("jrc_forest_loss_by_year__ha"),
        sum("negligible_risk_area__ha").alias("negligible_risk_area__ha"),
        sum("protected_areas_area__ha").alias("protected_areas_area__ha"),
        sum("landmark_area__ha").alias("landmark_area__ha"),
        sum("total_area__ha").alias("total_area__ha"),
        max("status_code").alias("status_code"),
        concat_ws(", ", collect_list(when(col("location_error").isNotNull && col("location_error") =!= "", col("location_error")))).alias("location_error")
      )
  }
}
