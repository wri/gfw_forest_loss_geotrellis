package org.globalforestwatch.features

case class GadmFeatureId(country: String, admin1: Integer, admin2: Integer) extends FeatureId {
  override def toString: String = s"$country - $admin1 - $admin2"

}
