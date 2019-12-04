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

  def getFeature(i: Row): geotrellis.vector.Feature[Geometry, GadmFeatureId] = {
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

    var newDF = df

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

    isoFirst.foreach { firstLetter =>
      newDF = newDF.filter(substring($"gid_0", 0, 1) === firstLetter(0))
    }

    isoStart.foreach { startCode =>
      newDF = newDF.filter($"gid_0" >= startCode)
    }

    isoEnd.foreach { endCode =>
      newDF = newDF.filter($"gid_0" < endCode)
    }

    iso.foreach { isoCode =>
      newDF = newDF.filter($"gid_0" === isoCode)
    }

    admin1.foreach { admin1Code =>
      newDF = newDF.filter($"gid_1" === admin1Code)
    }

    admin2.foreach { admin2Code =>
      newDF = newDF.filter($"gid_2" === admin2Code)
    }

    newDF
  }
}
