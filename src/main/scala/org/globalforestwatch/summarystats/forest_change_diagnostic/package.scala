package org.globalforestwatch.summarystats

import frameless.TypedEncoder

package object forest_change_diagnostic {
  implicit def dataDoubleTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataDouble] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataDouble, String]

  implicit def dataBooleanTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataBoolean] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataBoolean, String]

  implicit def dataDoubleCategoryTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataDoubleCategory] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataDoubleCategory, String]

  implicit def dataLossYearlyCategoryTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataLossYearlyCategory] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataLossYearlyCategory, String]

  // Uses the injection defined in object ForestChangeDiagnosticDataLossApproxYearlyCategory
  // See https://typelevel.org/frameless/Injection.html
  implicit def dataLossApproxYearlyCategoryTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataLossApproxYearlyCategory] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataLossApproxYearlyCategory, String]
}
