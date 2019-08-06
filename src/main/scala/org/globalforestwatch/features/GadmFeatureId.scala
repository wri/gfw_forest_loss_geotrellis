package org.globalforestwatch.features

case class GadmFeatureId(country: String, admin1: String, admin2: String) extends FeatureId {
  override def toString: String = s"$country - $admin1 - $admin2"

  def adm1ToInt: Integer = {
    try {
      admin1.split("[.]")(1).split("[_]")(0).toInt
    } catch {
      case e: Exception => null
    }
  }

  def adm2ToInt: Integer = {
    try {
      admin2.split("[.]")(2).split("[_]")(0).toInt
    } catch {
      case e: Exception => null
    }
  }
}
