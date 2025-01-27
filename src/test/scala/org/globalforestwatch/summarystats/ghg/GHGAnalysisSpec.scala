package org.globalforestwatch.summarystats.ghg

import cats.data.NonEmptyList
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.globalforestwatch.features.{FeatureFilter, ValidatedFeatureRDD}
import org.globalforestwatch.summarystats.ValidatedLocation
import org.globalforestwatch.{TestEnvironment, ProTag}
import org.globalforestwatch.config.GfwConfig
import org.apache.spark.broadcast.Broadcast
import org.globalforestwatch.util.Util

class GHGAnalysisSpec extends TestEnvironment with DataFrameComparer {
  def ghgInputTsvPath = getClass.getResource("/ghg.tsv").toString()
  def gadm2YieldPath = getClass.getResource("/part_yield_spam_gadm2.csv").toString()
  def ghgExpectedOutputPath = getClass.getResource("/ghg-output").toString()

  def Ghg(features: RDD[ValidatedLocation[Geometry]], broadcastArray: Broadcast[Array[Array[String]]]) = {
    GHGAnalysis(
      features,
      kwargs = Map(
        "config" -> GfwConfig.get(),
        "backupYield" -> broadcastArray,
        "includeFeatureId" -> true)
    )
  }

  /** Function to update expected results when this test becomes invalid */
  def saveExpectedFcdResult(fcd: DataFrame): Unit = {
    fcd.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(GHGExport.csvOptions)
      .csv(path = ghgExpectedOutputPath)
  }

  def readExpectedFcdResult = {
    val csvFile = spark.read
      .options(GHGExport.csvOptions)
      .csv(ghgExpectedOutputPath)
    // status_code gets interpreted as string type, so cast
    // it to its correct integer type.
    csvFile.withColumn("status_code", col("status_code").cast(IntegerType))
  }

  it("matches recorded output for various locations and commodities", ProTag) {
    val ghgFeatures = ValidatedFeatureRDD(
      NonEmptyList.one(ghgInputTsvPath),
      "gfwpro_ext",
      FeatureFilter.empty,
      splitFeatures = true
    )
    val backupArray = Util.readFile(gadm2YieldPath)
    //val backupDF = spark.read
    //  .options(Map("header" -> "true", "delimiter" -> ",", "escape" -> "\""))
    //  .csv(gadm2YieldPath)
    val broadcastArray = spark.sparkContext.broadcast(backupArray)
    val fcd = Ghg(ghgFeatures, broadcastArray)
    val summaryDF = GHGDF.getFeatureDataFrame(fcd, spark)
    summaryDF.collect().foreach(println)
    //saveExpectedFcdResult(summaryDF)

    val expectedDF = readExpectedFcdResult

    assertSmallDataFrameEquality(summaryDF, expectedDF, orderedComparison = false)
  }
}

