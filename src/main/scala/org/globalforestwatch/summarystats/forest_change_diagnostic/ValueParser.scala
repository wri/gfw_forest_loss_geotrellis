package org.globalforestwatch.summarystats.forest_change_diagnostic

trait ValueParser {
  val value: Any
  override def toString() = value.toString
}
