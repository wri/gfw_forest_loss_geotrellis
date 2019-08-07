package org.globalforestwatch.features

import geotrellis.vector.Geometry
import org.apache.spark.sql.Row

object FeatureFactory {

  def isValidGeom(featureName: String, i: Row): Boolean = {

    featureName match {
      case "gadm"    => GadmFeature.isValidGeom(i)
      case "feature" => SimpleFeature.isValidGeom(i)
      case _ =>
        throw new IllegalArgumentException(
          "Feature type must be one of 'gadm' and 'feature'"
        )
    }
  }

  def getFeature(featureName: String,
                 i: Row): geotrellis.vector.Feature[Geometry, FeatureId] = {
    featureName match {
      case "gadm"    => GadmFeature.getFeature(i)
      case "feature" => SimpleFeature.getFeature(i)
      case _ =>
        throw new IllegalArgumentException(
          "Feature type must be one of 'gadm' and 'feature'"
        )
    }
  }
}
