package org.globalforestwatch.summarystats

import cats.data.{NonEmptyList, Validated}
import com.monovore.decline.{Argument, Opts}
import cats.data.NonEmptyList
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.util.Config
import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions.{substring, col}

trait SummaryCommand {
  import SummaryCommand._

  val gfwPro: Opts[Boolean] = Opts
    .flag("gfwpro", "Feature flag for PRO, changes landcover labels")
    .orFalse


  implicit val configArgument: Argument[Config] = new Argument[Config] {
    def read(string: String) = {
      string.split(":", 2) match {
        case Array(key, value) => Validated.valid(Config(key, value))
        case _ => Validated.invalidNel(s"Invalid key:value pair: $string")
      }
    }

    def defaultMetavar = "key:value"
  }

  val featuresOpt: Opts[NonEmptyList[String]] =
    Opts.options[String]("features", "URI of features in TSV format")

  val outputOpt: Opts[String] =
    Opts.option[String]("output", "URI of output dir for CSV files")

  val overwriteOutputOpt: Opts[Boolean] = Opts
    .flag(
      "overwrite",
      help = "Overwrite output location if already existing"
    )
    .orFalse

  val featureTypeOpt: Opts[String] = Opts
    .option[String](
      "feature_type",
      help = "Feature type: one of 'gadm', 'wdpa', 'geostore', 'gfwpro' or 'feature'"
    )
    .withDefault("feature")

  val splitFeatures: Opts[Boolean] = Opts
    .flag("split_features", "Split input features along 1x1 degree grid")
    .orFalse

  val limitOpt: Opts[Option[Int]] = Opts
    .option[Int]("limit", help = "Limit number of records processed")
    .orNone

  val isoFirstOpt: Opts[Option[String]] =
    Opts
      .option[String]("iso_first", help = "Filter by first letter of ISO code")
      .orNone

  val isoStartOpt: Opts[Option[String]] =
    Opts
      .option[String](
        "iso_start",
        help = "Filter by ISO code larger than or equal to given value"
      )
      .orNone

  val isoEndOpt: Opts[Option[String]] =
    Opts
      .option[String](
        "iso_end",
        help = "Filter by ISO code smaller than given value"
      )
      .orNone

  val isoOpt: Opts[Option[String]] =
    Opts.option[String]("iso", help = "Filter by country ISO code").orNone

  val admin1Opt: Opts[Option[String]] = Opts
    .option[String]("admin1", help = "Filter by country Admin1 code")
    .orNone

  val admin2Opt: Opts[Option[String]] = Opts
    .option[String]("admin2", help = "Filter by country Admin2 code")
    .orNone

  val idStartOpt: Opts[Option[Int]] =
    Opts
      .option[Int](
        "id_start",
        help = "Filter by IDs larger than or equal to given value"
      )
      .orNone

  val idEndOpt: Opts[Option[Int]] =
    Opts
      .option[Int]("id_end", help = "Filter by IDs smaller than given value")
      .orNone

  val iucnCatOpts: Opts[Option[NonEmptyList[String]]] =
    Opts
      .options[String]("iucn_cat", help = "Filter by IUCN Category")
      .orNone

  val wdpaStatusOpts: Opts[Option[NonEmptyList[String]]] =
    Opts
      .options[String]("wdpa_status", help = "Filter by WDPA Status")
      .orNone

  val tclOpt: Opts[Boolean] = Opts.flag("tcl", "TCL tile extent").orFalse

  val gladOpt: Opts[Boolean] = Opts.flag("glad", "GLAD tile extent").orFalse

  val fireAlertTypeOpt: Opts[String] = Opts
    .option[String]("fire_alert_type", help = "modis or viirs")
    .withDefault("viirs")

  val requiredFireAlertSourceOpt: Opts[NonEmptyList[String]] = Opts
    .options[String](
      "fire_alert_source",
      help = "URI of fire alerts in TSV format"
    )

  val optionalFireAlertSourceOpt: Opts[Option[NonEmptyList[String]]] = Opts
    .options[String](
      "fire_alert_source",
      help = "URI of fire alerts in TSV format"
    ).orNone


  val noOutputPathSuffixOpt: Opts[Boolean] = Opts.flag("no_output_path_suffix", help = "Do not autogenerate output path suffix at runtime").orFalse

  val defaultOptions: Opts[BaseOptions] =
    (featureTypeOpt, featuresOpt, outputOpt, overwriteOutputOpt, splitFeatures, noOutputPathSuffixOpt).mapN(BaseOptions)

  val requiredFireAlertOptions: Opts[RequiredFireAlert] =
    (fireAlertTypeOpt, requiredFireAlertSourceOpt).mapN(RequiredFireAlert)

  val optionalFireAlertOptions: Opts[OptionalFireAlert] =
    (fireAlertTypeOpt, optionalFireAlertSourceOpt).mapN(OptionalFireAlert)

  val defaultFilterOptions: Opts[BaseFilter] =
    (tclOpt, gladOpt).mapN(BaseFilter)

  val gdamFilterOptions: Opts[GadmFilter] =
    (isoOpt, isoFirstOpt, isoStartOpt, isoEndOpt, admin1Opt, admin2Opt).mapN(GadmFilter)

  val wdpaFilterOptions : Opts[WdpaFilter] =
    (wdpaStatusOpts, iucnCatOpts).mapN(WdpaFilter)

  val featureIdFilterOptions: Opts[FeatureIdFilter] =
    (idStartOpt, idEndOpt).mapN(FeatureIdFilter)

  val pinnedVersionsOpts: Opts[Option[NonEmptyList[Config]]] = Opts.options[Config]("pin_version", "Pin version of contextual layer. Use syntax `--pin_version dataset:version`.").orNone

  val gadmVersOpt: Opts[String] =
    Opts.option[String]("gadm_version", help = "Gadm version for Pro analyses").withDefault("3.6")

  val featureFilterOptions: Opts[AllFilterOptions] = (
    defaultFilterOptions.orNone,
    featureIdFilterOptions.orNone,
    gdamFilterOptions.orNone,
    wdpaFilterOptions.orNone
  ).mapN(AllFilterOptions)

  def runAnalysis[A](analysis: SparkSession => A): A = {
    val name = getClass().getSimpleName()
    val spark = SummarySparkSession(name)
    try {
      analysis(spark)
    } finally {
      spark.stop()
    }
  }
}

object SummaryCommand {
  trait FilterOptions

  case class BaseOptions(
    featureType: String,
    featureUris: NonEmptyList[String],
    outputUrl: String,
    overwriteOutput: Boolean,
    splitFeatures: Boolean,
    noOutputPathSuffix: Boolean)

  case class BaseFilter(tcl: Boolean, glad: Boolean) extends FilterOptions {
    def filters(): List[Column] = {
      val trueValues: List[String] = List("t", "T", "true", "True", "TRUE", "1", "Yes", "yes", "YES")
      List(
        if (glad) Some(col("glad").isin(trueValues: _*)) else None,
        if (tcl) Some(col("tcl").isin(trueValues: _*)) else None
      ).flatten
    }
  }

  case class GadmFilter(
    iso: Option[String],
    isoFirst: Option[String],
    isoStart: Option[String],
    isoEnd: Option[String],
    admin1: Option[String],
    admin2: Option[String]
  ) extends FilterOptions {
    def filters(isoColumn: Column, admin1Column: Column, admin2Column: Column): List[Column] = {
      List(
        iso.map { code => isoColumn === code },
        isoStart.map { s => isoColumn >= s },
        isoEnd.map { s => isoColumn < s },
        isoFirst.map { s => substring(isoColumn, 0, 1) === s(0) },
        admin1.map { s => admin1Column === s },
        admin2.map { s => admin2Column === s }
      ).flatten
    }
  }

  case class WdpaFilter(
    wdpaStatus: Option[NonEmptyList[String]],
    iucnCat: Option[NonEmptyList[String]]
  ) extends FilterOptions {
    def filters(): List[Column] = {
      List(
        wdpaStatus.map { s => col("status") === s },
        iucnCat.map { s => col("iucn_cat") === s }
      ).flatten
    }
  }

  case class FeatureIdFilter(idStart: Option[Int], idEnd: Option[Int]) extends FilterOptions {
    def filters(idColumn: Column): List[Column] = {
      List(
        idStart.map { id => idColumn >= id },
        idEnd.map { id => idColumn <= id }
      ).flatten
    }
  }

  case class AllFilterOptions(
    base: Option[BaseFilter],
    featureId: Option[FeatureIdFilter],
    gadm: Option[GadmFilter],
    wdpa: Option[WdpaFilter])

  case class RequiredFireAlert(alertType: String, alertSource: NonEmptyList[String])

  case class OptionalFireAlert(alertType: String, alertSource: Option[NonEmptyList[String]])
}
