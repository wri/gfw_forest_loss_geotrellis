package org.globalforestwatch.features

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.substring

object GadmFeatureFilter extends FeatureFilter {

  def filter(isoFirst: Option[String],
             isoStart: Option[String],
             isoEnd: Option[String],
             iso: Option[String],
             admin1: Option[String],
             admin2: Option[String],
             limit: Option[Int],
             tcl: Boolean,
             glad: Boolean)(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    var newDF = df

    if (glad) newDF = newDF.filter($"glad" === "t")

    if (tcl) newDF = newDF.filter($"tcl" === "t")

    isoFirst.foreach { firstLetter =>
      newDF = newDF.filter(substring($"iso", 0, 1) === firstLetter(0))
    }

    isoStart.foreach { startCode =>
      newDF = newDF.filter($"iso" >= startCode)
    }

    isoEnd.foreach { endCode =>
      newDF = newDF.filter($"iso" < endCode)
    }

    iso.foreach { isoCode =>
      newDF = newDF.filter($"iso" === isoCode)
    }

    admin1.foreach { admin1Code =>
      newDF = newDF.filter($"gid_1" === admin1Code)
    }

    admin2.foreach { admin2Code =>
      newDF = newDF.filter($"gid_2" === admin2Code)
    }

    limit.foreach { n =>
      newDF = newDF.limit(n)
    }

    newDF
  }

}
