package org.globalforestwatch.features

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.globalforestwatch.util.GeometryReducer
import org.globalforestwatch.util.Util._

object GadmFeature extends Feature {

  val countryPos = 1
  val adm1Pos = 2
  val adm2Pos = 3
  val geomPos = 7

  def get(i: Row): geotrellis.vector.Feature[Geometry, GadmFeatureId] = {
    val countryCode: String = i.getString(countryPos)
    val admin1: Integer = try {
      i.getString(adm1Pos).split("[.]")(1).split("[_]")(0).toInt
    } catch {
      case e: Exception => null
    }

    val admin2: Integer = try {
      i.getString(adm2Pos).split("[.]")(2).split("[_]")(0).toInt
    } catch {
      case e: Exception => null
    }

    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )
    geotrellis.vector.Feature(geom, GadmFeatureId(countryCode, admin1, admin2))
  }

  override def custom_filter(
                              filters: Map[String, Any]
                            )(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val isoFirst: Option[String] =
      getAnyMapValue[Option[String]](filters, "isoFirst")
    val isoStart: Option[String] =
      getAnyMapValue[Option[String]](filters, "isoStart")
    val isoEnd: Option[String] =
      getAnyMapValue[Option[String]](filters, "isoEnd")
    val iso: Option[String] = getAnyMapValue[Option[String]](filters, "iso")
    val admin1: Option[String] =
      getAnyMapValue[Option[String]](filters, "admin1")
    val admin2: Option[String] =
      getAnyMapValue[Option[String]](filters, "admin2")

    val isoFirstDF: DataFrame = isoFirst.foldLeft(df)(
      (acc, i) => acc.filter(substring($"gid_0", 0, 1) === i(0))
    )
    val isoStartDF: DataFrame =
      isoStart.foldLeft(isoFirstDF)((acc, i) => acc.filter($"gid_0" >= i))
    val isoEndDF: DataFrame =
      isoEnd.foldLeft(isoStartDF)((acc, i) => acc.filter($"gid_0" < i))
    val isoDF: DataFrame =
      iso.foldLeft(isoEndDF)((acc, i) => acc.filter($"gid_0" === i))
    val admin1DF: DataFrame =
      admin1.foldRight(isoDF)((i, acc) => acc.filter($"gid_1" === i))
    admin2.foldRight(admin1DF)((i, acc) => acc.filter($"gid_2" === i))

  }
}
