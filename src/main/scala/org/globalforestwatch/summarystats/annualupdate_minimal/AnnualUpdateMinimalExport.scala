package org.globalforestwatch.summarystats.annualupdate_minimal

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.summarystats.annualupdate_minimal.dataframes._
import org.globalforestwatch.util.Util.getAnyMapValue

object AnnualUpdateMinimalExport extends SummaryExport {

  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    def exportWhitelist(df: DataFrame): Unit = {

      val adm2ApiDF = df.transform(Adm2ApiDF.whitelist)
      adm2ApiDF
        .coalesce(40) // TODO: optimize size so that tables have an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm2/whitelist")

      val adm1ApiDF = adm2ApiDF.transform(Adm1ApiDF.whitelist)
      adm1ApiDF
        .coalesce(12) // TODO: optimize size so that tables have an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm1/whitelist")

      val isoApiDF = adm1ApiDF.transform(IsoApiDF.whitelist)
      isoApiDF
        .coalesce(3) // TODO: optimize size so that tables have an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/iso/whitelist")

    }

    def exportSummary(df: DataFrame): Unit = {

      val adm2ApiDF = df.transform(Adm2ApiDF.sumArea)
      adm2ApiDF
        .coalesce(40) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm2/summary")

      val adm1ApiDF = adm2ApiDF.transform(Adm1ApiDF.sumArea)
      adm1ApiDF
        .coalesce(12) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm1/summary")

      val isoApiDF = adm1ApiDF.transform(IsoApiDF.sumArea)
      isoApiDF
        .coalesce(3) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/iso/summary")

    }

    def exportChange(df: DataFrame): Unit = {
      val adm2ApiDF = df
        .transform(Adm2ApiDF.sumChange)
        .coalesce(133) // this should result in an avg file size of 100MB

      adm2ApiDF.write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm2/change")

      val adm1ApiDF = adm2ApiDF
        .transform(Adm1ApiDF.sumChange)
        .coalesce(45) // this should result in an avg file size of 100MB

      adm1ApiDF.write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm1/change")

      val isoApiDF = adm1ApiDF
        .transform(IsoApiDF.sumChange)
        .coalesce(14) // this should result in an avg file size of 100MB

      isoApiDF.write
        .options(csvOptions)
        .csv(path = outputUrl + "/iso/change")
    }

    def exportDownload(df: DataFrame, outputUrl: String): Unit = {

      val spark = df.sparkSession
      import spark.implicits._

      val adm2SummaryDF = df
        .transform(Adm2DownloadDF.sumArea)

      adm2SummaryDF
        .transform(Adm2DownloadDF.roundValues)
        .coalesce(1)
        .orderBy(
          $"country",
          $"subnational1",
          $"subnational2",
          $"treecover_density__threshold"
        )
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm2/download")

      val adm1SummaryDF = adm2SummaryDF.transform(Adm1DownloadDF.sumArea)

      adm1SummaryDF
        .transform(Adm1DownloadDF.roundValues)
        .coalesce(1)
        .orderBy($"country", $"subnational1", $"treecover_density__threshold")
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm1/download")

      val isoSummaryDF = adm1SummaryDF.transform(IsoDownloadDF.sumArea)

      isoSummaryDF
        .transform(IsoDownloadDF.roundValues)
        .coalesce(1)
        .orderBy($"country", $"treecover_density__threshold")
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/iso/download")

    }

    val changeOnly: Boolean =
      getAnyMapValue[Boolean](kwargs, "changeOnly")

    val exportDF = summaryDF
      .transform(GadmDF.unpackValues)

    exportDF.cache()
    if (!changeOnly) {
      exportWhitelist(exportDF)
      exportSummary(exportDF)
      exportDownload(exportDF, outputUrl)
    }
    exportChange(exportDF)
    exportDF.unpersist()

  }

  override protected def exportWdpa(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    val changeOnly: Boolean =
      getAnyMapValue[Boolean](kwargs, "changeOnly")

    val exportDF = summaryDF
      .transform(WdpaDF.unpackValues)

    exportDF.cache()
    if (!changeOnly) {
      exportDF
        .transform(WdpaDF.whitelist)
        .coalesce(40) // TODO: optimize size so that tables have an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/wdpa/whitelist")

      exportDF
        .transform(WdpaDF.sumArea)
        .coalesce(40) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/wdpa/summary")
    }
    exportDF
      .transform(WdpaDF.sumChange)
      .coalesce(133) // this should result in an avg file size of 100MB
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/wdpa/change")

    exportDF.unpersist()
  }

  override protected def exportGeostore(summaryDF: DataFrame,
                                        outputUrl: String,
                                        kwargs: Map[String, Any]): Unit = {

    val changeOnly: Boolean =
      getAnyMapValue[Boolean](kwargs, "changeOnly")

    val exportDF = summaryDF
      .transform(GeostoreDF.unpackValues)

    exportDF.cache()
    if (!changeOnly) {

      exportDF
        .transform(GeostoreDF.whitelist)
        .coalesce(40) // TODO: optimize size so that tables have an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/geostore/whitelist")

      exportDF
        .transform(GeostoreDF.sumArea)
        .coalesce(40) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/geostore/summary")
    }
    exportDF
      .transform(GeostoreDF.sumChange)
      .coalesce(133) // this should result in an avg file size of 100MB
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/geostore/change")

    exportDF.unpersist()
  }
}