package org.globalforestwatch.summarystats


import cats.kernel.Semigroup
import cats.implicits._
import io.circe.syntax._
import io.circe.parser.decode


trait JobError

case class RasterReadError(msg: String) extends JobError
case class GeometryError(msg: String) extends JobError
case object NoIntersectionError extends JobError
case class MultiError(errors: Set[String]) extends JobError {
  def addError(err: JobError): MultiError = MultiError(errors + err.toString)
  def addError(other: MultiError): MultiError = MultiError(errors ++ other.errors)
}

object JobError {
  implicit def jobErrorSemigroup[A, B]: Semigroup[JobError] = Semigroup.instance {
    case (errs: MultiError, others: MultiError) => errs.addError(others)
    case (errs: MultiError, err) => errs.addError(err)
    case (err, errs: MultiError) => errs.addError(err)
    case (err1, err2) => MultiError(Set(err1.toString, err2.toString))
  }


  /** Convert from DataFrame error column encoding, will always produce MultiError instance */
  def fromErrorColumn(errors: String): Option[JobError] = {
    decode[List[String]](errors)
      .toOption
      .map { errs => MultiError(errs.toSet) }
  }

  /** Encode error as array of error strings */
  def toErrorColumn(jobError: JobError): String = {
    val errors: List[String] = jobError match {
      case MultiError(errs) => errs.toList.sorted
      case err => List(err.toString)
    }
    errors.asJson.noSpaces
  }
}
