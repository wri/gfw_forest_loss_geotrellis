package usbuildings

trait Comparable[A] {
  def compare(a: A, b: A): Boolean
}

object Implicits {
  implicit val intCompare = new Comparable[Int] {
    def compare(a: Int, b: Int): Boolean = a < b
  }

  implicit val strCompare = new Comparable[String] {
    def compare(a: String, b: String): Boolean = a < b
  }
}

class TypeClassSpec {
  import Implicits._

  val sort: (Int, Int) = Sort(1, 2)
  val sorted: (String, String) = Sort("a", "b")
}

object Sort {
  // sugared form: def apply[A: Comparable](a: A, b: A): (A, A) = {
  def apply[A](a: A, b: A)(implicit comp: Comparable[A]): (A, A) = {
    if (implicitly[Comparable[A]].compare(a, b)) {
      (a, b)
    } else {
      (b, a)
    }
  }
}


