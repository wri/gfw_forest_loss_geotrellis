addDependencyTreePlugin
addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.4")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0")
addSbtPlugin("net.pishen" % "sbt-lighter" % "1.2.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.8.2")
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.1.1")
addCompilerPlugin("org.scalameta" % "semanticdb-scalac" % "4.4.30" cross CrossVersion.full)