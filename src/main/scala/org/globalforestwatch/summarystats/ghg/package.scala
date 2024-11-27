package org.globalforestwatch.summarystats

import frameless.TypedEncoder

package object ghg {
  // Uses the injection defined in the companion object of each of these types.
  // See https://typelevel.org/frameless/Injection.html
  implicit def dataDoubleTypedEncoder: TypedEncoder[GHGDataDouble] =
    TypedEncoder.usingInjection[GHGDataDouble, String]

  implicit def dataValueYearlyTypedEncoder: TypedEncoder[GHGDataValueYearly] =
    TypedEncoder.usingInjection[GHGDataValueYearly, String]
}
