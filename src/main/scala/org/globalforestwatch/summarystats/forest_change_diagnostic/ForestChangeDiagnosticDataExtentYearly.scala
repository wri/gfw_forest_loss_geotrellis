package org.globalforestwatch.summarystats.forest_change_diagnostic

import scala.collection.immutable.SortedMap
import io.circe.syntax._

case class ForestChangeDiagnosticDataExtentYearly(value: SortedMap[Int, Double])
  extends ForestChangeDiagnosticDataParser[
    ForestChangeDiagnosticDataExtentYearly
  ] {
  def merge(
             other: ForestChangeDiagnosticDataExtentYearly
           ): ForestChangeDiagnosticDataExtentYearly = {

    ForestChangeDiagnosticDataExtentYearly(value ++ other.value.map {
      case (key, otherValue) =>
        key ->
          (value.getOrElse(key, 0.0) + otherValue)
    })
  }

  def toJson: String = {
    this.round.asJson.noSpaces
  }

  def round: SortedMap[Int, Double] = this.value.map {
    case (key, value) => key -> this.round(value)
  }
}

object ForestChangeDiagnosticDataExtentYearly {
  def fill(lossYear: Int,
           areaHa: Double,
           include: Boolean = true): ForestChangeDiagnosticDataExtentYearly = {

    // Only except lossYear values within range of default map or 0
    val minExtentYear: Int = this.prefilled.value.keysIterator.min
    val maxExtentYear: Int = this.prefilled.value.keysIterator.max

    if (lossYear == 0) {
      ForestChangeDiagnosticDataExtentYearly.prefilled.merge(
        ForestChangeDiagnosticDataExtentYearly(SortedMap(2000 -> areaHa))
      )
    } else if (minExtentYear <= lossYear && lossYear <= maxExtentYear && include) {

      val years: List[Int] = List.range(minExtentYear, maxExtentYear + 1)

      // convert a list of tuples to a sorted map
      val values: SortedMap[Int, Double] = SortedMap(
        years.map(
          year =>
            (
              year,
              if (year < lossYear) areaHa
              else 0
            )
        ): _*
      )

      ForestChangeDiagnosticDataExtentYearly(values)

    } else
      this.empty
  }

  def empty: ForestChangeDiagnosticDataExtentYearly =
    ForestChangeDiagnosticDataExtentYearly(SortedMap())

  def prefilled: ForestChangeDiagnosticDataExtentYearly =
    ForestChangeDiagnosticDataExtentYearly(
      SortedMap(
        2000 -> 0,
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

}
