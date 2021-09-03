package org.globalforestwatch.summarystats

import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Include this trait where Data classes are being convereted to Spark DataFrame or DataSet
 * The implicit defined here will superceded the reflection based process of deriving encoders used in Spark.
 *
 * For instance, if these encoders are not used and you write a DataFrame containing ForestChangeDiagnosticDataLossYearly to csv
 * you will see an error that says that csv format does nto support columns with map data type.
 * This is because the Injection from ForestChangeDiagnosticDataLossYearly to JSON string was not found and used.
 */
trait FramelessEncoders {

  /** High priority specific product encoder derivation. Without it, the default spark would be used. */
  implicit def productEncoder[T <: Product: TypedEncoder]: ExpressionEncoder[T] =
    TypedExpressionEncoder[T].asInstanceOf[ExpressionEncoder[T]]
}