package org.globalforestwatch.summarystats

import frameless.TypedEncoder

package object forest_change_diagnostic {
  // Uses the injection defined in the companion object of each of these types.
  // See https://typelevel.org/frameless/Injection.html
  implicit def dataDoubleTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataDouble] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataDouble, String]

  implicit def dataBooleanTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataBoolean] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataBoolean, String]

  implicit def dataDoubleCategoryTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataDoubleCategory] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataDoubleCategory, String]

  implicit def dataLossYearlyCategoryTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataLossYearlyCategory] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataLossYearlyCategory, String]

  implicit def dataLossApproxYearlyCategoryTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataLossApproxYearlyCategory] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataLossApproxYearlyCategory, String]

  implicit def dataLossApproxYearlyTwoCategoryTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataLossApproxYearlyTwoCategory] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataLossApproxYearlyTwoCategory, String]

  implicit def dataLossYearlyTwoCategoryTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataLossYearlyTwoCategory] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataLossYearlyTwoCategory, String]

  implicit def dataDoubleTwoCategoryTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataDoubleTwoCategory] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataDoubleTwoCategory, String]

}
