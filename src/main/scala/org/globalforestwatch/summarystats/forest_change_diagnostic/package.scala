package org.globalforestwatch.summarystats

import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

package object forest_change_diagnostic {
  /** Use TypedExpressionEncoder and upcast derived Encoder to ExpressionEncoder */
  implicit def typedExpressionEncoder[T: TypedEncoder]: ExpressionEncoder[T] =
    TypedExpressionEncoder[T].asInstanceOf[ExpressionEncoder[T]]

  /** High priority specific product encoder derivation. Without it, the default spark would be used. */
  implicit def productEncoder[T <: Product: TypedEncoder]: ExpressionEncoder[T] = typedExpressionEncoder


  implicit def dataDoubleTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataDouble] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataDouble, String]

  implicit def dataBooleanTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataBoolean] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataBoolean, String]

  implicit def dataDoubleCategoryTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataDoubleCategory] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataDoubleCategory, String]

  implicit def dataLossYearlyCategoryTypedEncoder: TypedEncoder[ForestChangeDiagnosticDataLossYearlyCategory] =
    TypedEncoder.usingInjection[ForestChangeDiagnosticDataLossYearlyCategory, String]

  implicit def dataExpressionEncoder: ExpressionEncoder[ForestChangeDiagnosticData] =
    typedExpressionEncoder[ForestChangeDiagnosticData]
}
