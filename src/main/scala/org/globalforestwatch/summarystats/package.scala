package org.globalforestwatch

import cats.kernel.Semigroup
import cats.data.Validated
import geotrellis.raster.summary.polygonal.PolygonalSummaryResult

package object summarystats {
  type ValidatedRow[A] = Validated[JobError, A]
  type ValidatedSummary[A] = Validated[JobError, PolygonalSummaryResult[A]]


  implicit def summarySemigroup[SUMMARY <: Summary[SUMMARY]]: Semigroup[SUMMARY] = Semigroup.instance {
    case (s1, s2) => s1.merge(s2)
  }
}
