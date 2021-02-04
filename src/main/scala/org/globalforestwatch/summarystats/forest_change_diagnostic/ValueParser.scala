package org.globalforestwatch.summarystats.forest_change_diagnostic

trait ValueParser[Self <: ValueParser[Self]] {
  val value: Any

  def merge(other: Self): Self

  def toJson: String

  def round(value: Double, digits: Int = 4): Double = {
    Math.round(value * math.pow(10, digits)) / math.pow(10, digits)
  }
}
