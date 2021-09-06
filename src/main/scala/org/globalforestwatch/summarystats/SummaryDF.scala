package org.globalforestwatch.summarystats

import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Extend this trait where Data classes are being convereted to Spark DataFrame or DataSet to produce a Summary DataFrame
 * The implicit defined here will superceded the reflection based process of deriving encoders used in Spark.
 *
 * For instance, if these encoders are not used and you write a DataFrame containing ForestChangeDiagnosticDataLossYearly to csv
 * you will see an error that says that csv format does nto support columns with map data type.
 * This is because the Injection from ForestChangeDiagnosticDataLossYearly to JSON string was not found and used.
 */
trait SummaryDF {
  /** High priority specific product encoder derivation. Without it, the default spark would be used. */
  implicit def productEncoder[T <: Product: TypedEncoder]: ExpressionEncoder[T] =
    TypedExpressionEncoder[T].asInstanceOf[ExpressionEncoder[T]]
}

object SummaryDF  {
  case class RowId(list_id: String, location_id: String)

  case class RowError(status_code: Int, location_error: String)
  object RowError {
    val empty: RowError = RowError(status_code = 2, location_error = null)
    def fromJobError(err: JobError): RowError = RowError(
      status_code = 3,
      location_error = JobError.toErrorColumn(err)
    )
  }
}