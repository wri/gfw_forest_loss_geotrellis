package org.globalforestwatch.summarystats.forest_change_diagnostic


import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class ForestChangeDiagnosticRawData(
                                          var totalArea: Double,

                                        ) {
  def merge(other: ForestChangeDiagnosticRawData): ForestChangeDiagnosticRawData = {

    ForestChangeDiagnosticRawData(

      totalArea + other.totalArea,

    )
  }
}

object ForestChangeDiagnosticRawData {
  implicit val lossDataSemigroup: Semigroup[ForestChangeDiagnosticRawData] =
    new Semigroup[ForestChangeDiagnosticRawData] {
      def combine(x: ForestChangeDiagnosticRawData,
                  y: ForestChangeDiagnosticRawData): ForestChangeDiagnosticRawData =
        x.merge(y)
    }

}
