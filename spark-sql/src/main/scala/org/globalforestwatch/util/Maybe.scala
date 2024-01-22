package org.globalforestwatch.util

import cats.Applicative
import cats.Functor
import cats.Monoid
import org.apache.spark.sql.{Column, DataFrame, Encoders, functions => F}
import org.apache.spark.sql.types.StructType
import org.globalforestwatch.udf._

import scala.reflect.runtime.universe.TypeTag
import scala.util.{Try, Success, Failure}

case class Maybe[A](content: Option[A], error: String) {
  def mapWithTry[B](fn: A => B): Maybe[B] = {
    Try(content.map(fn)) match {
      // A Success means the mapping did not throw, and either Some(v) gives the result
      // This encompasses starting from an error (None) state
      case Success(opt) => Maybe(opt, error)
      // Failure means we threw, and we should switch to an error state
      case Failure(thrown) => Maybe(None, thrown.getMessage)
    }
  }

  def mapWithEither[B](fn: A => Either[String, B]): Maybe[B] = content.map(fn) match {
    case Some(Left(newError)) => Maybe(None, f"${error}, ${newError}")
    case Some(Right(content)) => Maybe(Some(content), error)
    case None => Maybe(None, error)
  }

  def mapWithOption[B](fn: A => Option[B], errMsg: String): Maybe[B] =
    content.map(fn) match {
      // Our mapping failed, create a new error with our message
      case Some(None) => Maybe(None, errMsg)
      // Successful mapping (error should be null, but send it down anyway)
      case Some(v) => Maybe(v, error)
      // Previous error, propagate it
      case None => Maybe(None, error)
    }

  def assert(cond: A => Boolean, errMsg: String): Maybe[A] = {
    val lifted = { x: A => if (cond(x)) { Some(x) } else { None } }
    mapWithOption(lifted, errMsg)
  }

}

object Maybe {
  def apply[A](x: A): Maybe[A] = Maybe(Some(x), null)

  def error[A](err: String): Maybe[A] = Maybe(None, err)

  def throwing[A](x: => A): Maybe[A] = Try(x) match {
    case Success(v) => Maybe(v)
    case Failure(thrown) => Maybe.error(thrown.getMessage)
  }

  def either[A](x: => Either[String, A]): Maybe[A] = x match {
    case Left(err) => Maybe.error(err)
    case Right(v) => Maybe(v)
  }

  def option[A](errMsg: String)(x: => Option[A]): Maybe[A] = x match {
    case Some(v) => Maybe(v)
    case None => Maybe.error(errMsg)
  }

  def filter[A](errMsg: String, pred: A => Boolean)(x: => A): Maybe[A] = {
    if (x != null && pred(x))
      Maybe(x)
    else
      Maybe.error(errMsg)
  }

  def wrapWithError[T <: Product : TypeTag](errorColumn: Column, columns: Column*): Column = {
    val schemaInner = Encoders.product[T].schema
    val schemaOuter = Encoders.product[Maybe[T]].schema

    val innerStructFields = columns.zip(schemaInner.fields).map {
      case (column, field) => column.cast(field.dataType).as(field.name)
    }
    val inner = F.struct(innerStructFields: _*).as("content")

    F.struct(inner, errorColumn.as("error"))
  }

  def unwrap(df: DataFrame, maybeColumnName: String, errorColumnName: String = "error"): DataFrame = {
    val residFields =
      df.columns.diff(Seq(errorColumnName, maybeColumnName)).map(F.col(_))

    val outer = (df.schema.find(_.name == maybeColumnName) match {
      case Some(v) => v.dataType
      case None => throw new NoSuchFieldException(
        f"Could not find column `${maybeColumnName}` during unwrap operation"
      )
    }).asInstanceOf[StructType]

    val inner = outer.find(_.name == "content") match {
      case Some(v) => v.dataType
      case None => throw new NoSuchFieldException(
        f"Column `${maybeColumnName}` does not contain expected `content` field (is it of type Maybe[A]?)"
      )
    }

    val nestedFields: Seq[Column] = inner match {
      case s: StructType => s.map(f => F.col(maybeColumnName + ".content." + f.name).as(f"${maybeColumnName}.${f.name}"))
      case f => Seq(F.col(f"${maybeColumnName}.content").as(maybeColumnName))
    }

    val combinedErrorColumn = if (df.columns.contains(errorColumnName)) {
      concat_outer(F.lit("\n"), F.col(errorColumnName), F.col(maybeColumnName + ".error"))
    } else {
      F.col(maybeColumnName + ".error")
    }

    val allColumns = combinedErrorColumn.as(errorColumnName) +: (residFields ++ nestedFields)
    df.select(allColumns:_*)
  }

  implicit class MaybeMethods(df: DataFrame) {
    def unwrap(maybeColumnName: String, errorColumnName: String = "error"): DataFrame =
      Maybe.unwrap(df, maybeColumnName, errorColumnName)
  }

  implicit val maybeApplicativeInstance: Applicative[Maybe] = new Applicative[Maybe] {
    def pure[A](a: A) = Maybe(Some(a), null)
    def ap[A, B](ff: Maybe[A => B])(fa: Maybe[A]): Maybe[B] = ff match {
      case Maybe(Some(fn), _) => Maybe(fa.content.map(fn), fa.error)
      case Maybe(None, e) => Maybe.error(e)
    }
  }

  implicit val maybeFunctorInstance: Functor[Maybe] = new Functor[Maybe] {
    def map[A, B](fa: Maybe[A])(f: A => B): Maybe[B] = fa match {
      case Maybe(v, null) => Maybe(v.map(f), null)
      case Maybe(_, err) => Maybe(None, err)
    }
  }

  implicit def maybeMonoidInstance[T: Monoid]: Monoid[Maybe[T]] = new Monoid[Maybe[T]] {
    def empty: Maybe[T] = Maybe(Monoid[T].empty)
    def combine(a: Maybe[T], b: Maybe[T]): Maybe[T] = (a, b) match {
      case (Maybe(None, e1), Maybe(None, e2)) => Maybe.error(f"${e1}\n${e2}")
      case (Maybe(None, e1), _) => Maybe.error(e1)
      case (_, Maybe(None, e2)) => Maybe.error(e2)
      case (Maybe(Some(v1), _), Maybe(Some(v2), _)) => Maybe(Monoid[T].combine(v1, v2))
    }
  }
}
