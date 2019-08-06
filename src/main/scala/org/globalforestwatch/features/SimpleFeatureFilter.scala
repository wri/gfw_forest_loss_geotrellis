package org.globalforestwatch.features

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.substring

object SimpleFeatureFilter extends FeatureFilter {

  def filter(idStart: Option[Int], idEnd: Option[Int], limit: Option[Int])(
    spark: SparkSession
  )(df: DataFrame): DataFrame = {

    import spark.implicits._

    var newDF = df

    idStart.foreach { startId =>
      newDF = newDF.filter($"fid" >= startId)
    }

    idEnd.foreach { endId =>
      newDF = newDF.filter($"fid" < endId)
    }

    limit.foreach { n =>
      newDF = newDF.limit(n)
    }

    newDF
  }

}
