package org.globalforestwatch.summarystats.annualupdate_minimal

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.rdd.RDD
import cats.data.NonEmptyList
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.globalforestwatch.{TestEnvironment, DefaultTag}
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features.{FeatureId, FeatureRDD, FeatureFilter}

class AnnualUpdateMinimalSpec extends TestEnvironment with DataFrameComparer {
  def idn1_5GadmInputTsvPath = getClass.getResource("/idn1_5Gadm.tsv").toString()
  def idn1_5GadmExpectedOutputPath = getClass.getResource("/idn1_5Gadm-aum-output").toString()
  def wdpaInputTsvPath = getClass.getResource("/mulanje.tsv").toString()
  def wdpaExpectedOutputPath = getClass.getResource("/wdpa-aum-output").toString()
  
  
  val csvOptions: Map[String, String] = Map(
  "header" -> "true",
  "delimiter" -> "\t",
  "escape" -> "\"",
  "quoteMode" -> "MINIMAL",
  "nullValue" -> "",
  "emptyValue" -> ""
)

  def AnnualUpdateMinimal(inputPath: String, featureType: String): DataFrame = {
    
    // Create featureRDD
    val featureRDD = FeatureRDD(
      NonEmptyList.one(inputPath), 
      featureType, 
      FeatureFilter.empty,
      splitFeatures = true,
      spark)

    // Run AnnualUpdateMinimal Analysis
    val kwargs = Map("config" -> GfwConfig.get())
    val aumRDD: RDD[(FeatureId, AnnualUpdateMinimalSummary)] = 
      AnnualUpdateMinimalRDD(featureRDD, AnnualUpdateMinimalGrid.blockTileGrid, kwargs)
    val aumDFFactory = AnnualUpdateMinimalDFFactory(featureType, aumRDD, spark)
    val aumDF: DataFrame = aumDFFactory.getDataFrame

    aumDF
  }

  /** Function to update expected results when this test becomes invalid */
  def saveExpectedAumResult(aum: DataFrame, outputPath: String): Unit = {
    aum.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(AnnualUpdateMinimalExport.csvOptions)
      .csv(path = outputPath)
  }

  def readAumResult(path: String) = {
    spark.read
      .options(Map(
        "header" -> "true",
        "delimiter" -> "\t",
        "escape" -> "\"",
        "quoteMode" -> "MINIMAL",
        "nullValue" -> "",
        "emptyValue" -> "",
        "inferSchema" -> "true"
      ))
      .csv(path)
  }

  it("matches recorded output for GADM", DefaultTag) {
    val gadmDF: DataFrame = AnnualUpdateMinimal(idn1_5GadmInputTsvPath, "gadm")

    // Transform results to match expected output
    // Emulating AnnualUpdateMinimalExport.export_gadm()
    import spark.implicits._
    val unpackedDF = gadmDF      
      .transform(
        AnnualUpdateMinimalDF.unpackValues(
          List($"id.iso" as "iso", $"id.adm1" as "adm1", $"id.adm2" as "adm2")
        )
      )
    val exportDF: DataFrame = unpackedDF.transform(AnnualUpdateMinimalDF.aggSummary(List("iso", "adm1", "adm2"))) 
    val top20Rows = exportDF.limit(20) // Due to size of output, compare top 20 rows
    
    // Uncomment to save new expected results
    //saveExpectedAumResult(top20Rows, idn1_5GadmExpectedOutputPath)

    // Write results to CSV (ensure that nulls are read the same way as expected results)
    top20Rows
      .write
      .options(csvOptions)
      .csv("output/gadm-aum-output")

    // Read expected results and compare
    val expectedDF = readAumResult(idn1_5GadmExpectedOutputPath)
    val top20RowsDF = readAumResult("output/gadm-aum-output")

    assertApproximateDataFrameEquality(top20RowsDF, expectedDF, 0.00001, ignoreNullable = true)

  }

  it("matches recorded output for WDPA", DefaultTag) {
    val wdpaDF: DataFrame = AnnualUpdateMinimal(wdpaInputTsvPath, "wdpa")

    // Transform results to match expected output
    // Emulating AnnualUpdateMinimalExport.export_wdpa()
    import spark.implicits._
    val idCols: List[String] = List(
      "wdpa_protected_area__id",
      "wdpa_protected_area__name",
      "wdpa_protected_area__iucn_cat",
      "wdpa_protected_area__iso",
      "wdpa_protected_area__status"
    )
    val unpackedDF = wdpaDF      
      .transform(
        AnnualUpdateMinimalDF.unpackValues(
          List(
            $"id.wdpaId" as "wdpa_protected_area__id",
            $"id.name" as "wdpa_protected_area__name",
            $"id.iucnCat" as "wdpa_protected_area__iucn_cat", 
            $"id.iso" as "wdpa_protected_area__iso",
            $"id.status" as "wdpa_protected_area__status"
          ),
          wdpa = true
        )
      )
    val exportDF = unpackedDF.transform(AnnualUpdateMinimalDF.aggSummary(idCols, wdpa=true))
    val top20Rows = exportDF.limit(20) // Due to size of output, compare top 20 rows

    // Uncomment to save new expected results
    //saveExpectedAumResult(top20Rows, wdpaExpectedOutputPath)

    // Write results to CSV (ensure that nulls are read the same way as expected results)
    top20Rows
      .write
      .options(csvOptions)
      .csv("output/wdpa-aum-output")

    // Read expected results and compare
    val expectedDF = readAumResult(wdpaExpectedOutputPath)
    val top20RowsDF = readAumResult("output/wdpa-aum-output")

    assertApproximateDataFrameEquality(top20RowsDF, expectedDF, 0.00001, ignoreNullable = true)
  }
}
