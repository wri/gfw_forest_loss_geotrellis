package org.globalforestwatch.summarystats.forest_change_diagnostic

import org.globalforestwatch.util.Implicits._
import scala.collection.immutable.SortedMap
import io.circe.syntax._

case class ForestChangeDiagnosticDataLossYearly(value: SortedMap[Int, Double]) extends ForestChangeDiagnosticDataParser[ForestChangeDiagnosticDataLossYearly] {
  def merge(
             other: ForestChangeDiagnosticDataLossYearly
           ): ForestChangeDiagnosticDataLossYearly = {

    ForestChangeDiagnosticDataLossYearly(value ++ other.value.map {
      case (key, otherValue) =>
        key ->
          (value.getOrElse(key, 0.0) + otherValue)
    })
  }

  def round: SortedMap[Int, Double] = this.value.map { case (key, value) => key -> this.round(value) }

  def toJson: String = {
    this.round.asJson.noSpaces
  }
}

object ForestChangeDiagnosticDataLossYearly {
  def empty: ForestChangeDiagnosticDataLossYearly =
    ForestChangeDiagnosticDataLossYearly(
      SortedMap(
        2001 -> 0,
        2002 -> 0,
        2003 -> 0,
        2004 -> 0,
        2005 -> 0,
        2006 -> 0,
        2007 -> 0,
        2008 -> 0,
        2009 -> 0,
        2010 -> 0,
        2011 -> 0,
        2012 -> 0,
        2013 -> 0,
        2014 -> 0,
        2015 -> 0,
        2016 -> 0,
        2017 -> 0,
        2018 -> 0,
        2019 -> 0
      )
    )

  def fill(lossYear: Int,
           areaHa: Double,
           include: Boolean = true): ForestChangeDiagnosticDataLossYearly = {

    // Only except lossYear values within range of default map
    val minLossYear: Int = this.empty.value.keysIterator.min
    val maxLossYear: Int = this.empty.value.keysIterator.max

    if (minLossYear <= lossYear && lossYear <= maxLossYear)
      ForestChangeDiagnosticDataLossYearly(
        SortedMap(
          lossYear ->
            areaHa * include
        )
      )
    else
      this.empty
  }

}
