package workshop

/** This is comparable type class interface */
trait Comparable[A] {
  def compare(a: A, b: A): Boolean
}

object Implicits {
  /** These are instances of type class for specific types */
  implicit val intCompare = new Comparable[Int] {
    def compare(a: Int, b: Int): Boolean = a < b
  }

  implicit val strCompare = new Comparable[String] {
    def compare(a: String, b: String): Boolean = a < b
  }
}

class TypeClassSpec {
  import Implicits._

  /** At call site the compile will implicitly find instances of Comparable[Int] and Comparable[String] *//
  val sort: (Int, Int) = Sort(1, 2)
  val sorted: (String, String) = Sort("a", "b")

  // Compiler will expand above to:
  // val sort: (Int, Int) = Sort(1, 2)(intCompare)
  // val sorted: (String, String) = Sort("a", "b")(strCompare)
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


