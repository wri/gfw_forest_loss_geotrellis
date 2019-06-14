package org.globalforestwatch.features

case class GADMFeatureId(country: String, admin1: String, admin2: String) {
  override def toString: String = s"$country - $admin1 - $admin2"
}


