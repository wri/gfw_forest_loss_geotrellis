package org.globalforestwatch.summarystats.firealerts

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features._
import org.globalforestwatch.util.Util.getAnyMapValue

import scala.collection.immutable

case class FireAlertsDFFactory(
                                featureType: String,
                                summaryRDD: RDD[(FeatureId, FireAlertsSummary)],
                                spark: SparkSession,
                                kwargs: Map[String, Any]
) {
  val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")

  import spark.implicits._

  def getDataFrame: DataFrame = {
    fireAlertType match {
      case "viirs" => featureType match {
        case "gadm" => getViirsGadmDataFrame
        case "wdpa" => getViirsWdpaDataFrame
        case "geostore" => getViirsGeostoreDataFrame
        case "simple" => getViirsSimpleDataFrame
        case _ =>
          throw new IllegalArgumentException("Not a valid feature type")
      }
      case "modis" => featureType match {
        case "gadm" => getModisGadmDataFrame
        case "wdpa" => getModisWdpaDataFrame
        case "geostore" => getModisGeostoreDataFrame
        case "simple" => getModisSimpleDataFrame
        case _ =>
          throw new IllegalArgumentException("Not a valid feature type")
      }
      case _ =>
        throw new IllegalArgumentException("Not a valid fire alert type")
    }
  }

  def getViirsGadmDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case CombinedFeatureId(viirsId: ViirsFireAlertFeatureId, gadmId: GadmFeatureId) =>
                  FireAlertsRowViirsGadm(viirsId, gadmId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a valid Fire Alert ID")
              }
            }
          }
      }
      .toDF("fireId", "featureId", "data_group", "data")
  }

  def getViirsWdpaDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case CombinedFeatureId(viirsId: ViirsFireAlertFeatureId, wdpaId: WdpaFeatureId) =>
                  FireAlertsRowViirsWdpa(viirsId, wdpaId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a valid Fire Alert ID")
              }
            }
          }
      }
      .toDF("fireId", "featureId", "data_group", "data")
  }

  def getViirsGeostoreDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case CombinedFeatureId(viirsId: ViirsFireAlertFeatureId, geostoreId: GeostoreFeatureId) =>
                  FireAlertsRowViirsGeostore(viirsId, geostoreId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a valid Fire Alert ID")
              }
            }
          }
      }
      .toDF("fireId", "featureId", "data_group", "data")
  }

  def getViirsSimpleDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case CombinedFeatureId(viirsId: ViirsFireAlertFeatureId, simpleId: SimpleFeatureId) =>
                  FireAlertsRowViirsSimple(viirsId, simpleId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a valid Fire Alert ID")
              }
            }
          }
      }
      .toDF("fireId", "featureId", "data_group", "data")
  }

  def getModisGadmDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case CombinedFeatureId(modisId: ModisFireAlertFeatureId, gadmId: GadmFeatureId) =>
                  FireAlertsRowModisGadm(modisId, gadmId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a valid Fire Alert ID")
              }
            }
          }
      }
      .toDF("fireId", "featureId", "data_group", "data")
  }

  def getModisWdpaDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case CombinedFeatureId(modisId: ModisFireAlertFeatureId, wdpaId: WdpaFeatureId) =>
                  FireAlertsRowModisWdpa(modisId, wdpaId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a valid Fire Alert ID")
              }
            }
          }
      }
      .toDF("fireId", "featureId", "data_group", "data")
  }

  def getModisGeostoreDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case CombinedFeatureId(modisId: ModisFireAlertFeatureId, geostoreId: GeostoreFeatureId) =>
                  FireAlertsRowModisGeostore(modisId, geostoreId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a valid Fire Alert ID")
              }
            }
          }
      }
      .toDF("fireId", "featureId", "data_group", "data")
  }

  def getModisSimpleDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case CombinedFeatureId(modisId: ModisFireAlertFeatureId, simpleId: SimpleFeatureId) =>
                  FireAlertsRowModisSimple(modisId, simpleId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a valid Fire Alert ID")
              }
            }
          }
      }
      .toDF("fireId", "featureId", "data_group", "data")
  }
}
