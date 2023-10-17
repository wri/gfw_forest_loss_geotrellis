package org.globalforestwatch.features

case class BurnedAreasFeatureId(alertDate: String) extends FeatureId {
  override def toString: String = alertDate
}
