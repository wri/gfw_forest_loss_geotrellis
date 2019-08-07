package org.globalforestwatch.features

import geotrellis.vector.Geometry
import org.apache.spark.sql.Row

case class FeatureFactory(featureName: String) {

  private val featureObj: Feature = featureName match {
    case "gadm" => GadmFeature
    case "feature" => SimpleFeature
    case "wdpa" => WdpaFeature
    case _ =>
      throw new IllegalArgumentException(
        "Feature type must be one of 'gadm' and 'feature'"
      )
  }

  def isValidGeom(i: Row): Boolean = {
    featureObj.isValidGeom(i)
  }

  def getFeature(i: Row): geotrellis.vector.Feature[Geometry, FeatureId] = {
    featureObj.getFeature(i)
  }
}
