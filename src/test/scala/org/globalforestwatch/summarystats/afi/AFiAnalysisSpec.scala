package org.globalforestwatch.summarystats.afi

import cats.data.{NonEmptyList, Validated}
import com.github.mrpowers.spark.daria.sql.transformations.sortColumns
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.globalforestwatch.{TestEnvironment, ProTag}
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features.{FeatureFilter, GfwProFeatureId, ValidatedFeatureRDD}
import org.globalforestwatch.summarystats.{Location, ValidatedLocation}

class AFiAnalysisSpec extends TestEnvironment with DataFrameComparer {
  def palm32InputTsvPath = getClass.getResource("/palm-oil-32.tsv").toString()
  def palm32ExpectedOutputPath = getClass.getResource("/palm-32-afi-output").toString()

  def AFI(features: RDD[ValidatedLocation[Geometry]]): DataFrame = {
    AFiAnalysis(
      features,
      "gfwpro",
      spark,
      kwargs = Map("config" -> GfwConfig.get())
    )
  }

  /** Function to update expected results when this test becomes invalid */
  def saveExpectedAFiResult(fcd: DataFrame): Unit = {
    fcd
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(AFiExport.csvOptions)
      .csv(path = palm32ExpectedOutputPath)
  }

  def readExpectedAFiResult = {
    spark.read
      .options(Map(
        "header" -> "true",
        "delimiter" -> "\t",
        "quote" -> "\u0000",
        "escape" -> "\u0000",
        "quoteMode" -> "NONE",
        "nullValue" -> "",
        "emptyValue" -> "",
        "inferSchema" -> "true"
      ))
      .csv(palm32ExpectedOutputPath)
      .withColumn("status_code", col("status_code").cast(IntegerType))
      .withColumn("gadm_id", when(col("gadm_id").isNull, lit("")).otherwise(col("gadm_id")))
      .withColumn("location_error", lit(""))
  }

  it("matches recorded output for palm oil mills location 31", ProTag) {
    val featureLoc31RDD = ValidatedFeatureRDD(
      NonEmptyList.one(palm32InputTsvPath),
      "gfwpro",
      FeatureFilter.empty,
      splitFeatures = true
    ).filter {
      case Validated.Valid(Location(GfwProFeatureId(_, 31), _)) => true
      case _ => false
    }

    // Duplicate the single feature row, but change the location id to -1, so it is
    // the dissolved summary. This exercises the code that creates extra dissolved
    // result rows, including by gadm_id.
    val augRDD = featureLoc31RDD.mapPartitions {partition => 
      val data = partition.toList
      (data ++ data.map { row =>
        row match {
          case Validated.Valid(Location(GfwProFeatureId(listId, locationId), geom)) => Validated.Valid(Location(GfwProFeatureId(listId, -1), geom))
          case r => r
        }
      }).iterator
    }

    val afiDF = AFI(augRDD)
    //saveExpectedAFiResult(afiDF)

    val expectedDF = readExpectedAFiResult

    afiDF.show()
    expectedDF.show()

    assertSmallDataFrameEquality(afiDF.transform(sortColumns()), expectedDF.transform(sortColumns()), ignoreNullable = true)
  }
}
