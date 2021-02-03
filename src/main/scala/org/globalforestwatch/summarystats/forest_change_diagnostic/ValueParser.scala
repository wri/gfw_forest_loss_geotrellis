package org.globalforestwatch.summarystats.forest_change_diagnostic

trait ValueParser[Self <: ValueParser[Self]] {
  val value: Any

  def merge(other: Self): Self

  def toJson: String
}
