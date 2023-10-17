package org.globalforestwatch.summarystats

import org.globalforestwatch.features.FeatureId
import cats.Functor

object Location {
  def apply[A](id: FeatureId, thing: A): Tuple2[FeatureId, A] = (id, thing)

  def unapply[A](location: Location[A]): Option[Tuple2[FeatureId, A]] = Some(location)

  implicit class methods[A](location: Location[A]) {
    def id = location._1
    def thing = location._2
  }

  implicit val locationFunctor: Functor[Location] = new Functor[Location] {
    def map[A, B](fa: Location[A])(f: A => B): Location[B] = Location(fa.id, f(fa.thing))
  }
}

case class LocationError(id: FeatureId, error: JobError)
