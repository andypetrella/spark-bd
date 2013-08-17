scalaVersion := "2.9.3"

resolvers += "Sonatype release" at "https://oss.sonatype.org/content/repositories/releases"

resolvers += "Akka repo" at "http://repo.akka.io/releases/"

resolvers += "Spray repo" at "http://repo.spray.cc"

libraryDependencies += "org.spark-project" %% "spark-core" % "0.7.3"

libraryDependencies += "org.spark-project" %% "spark-streaming" % "0.7.3"

libraryDependencies += "com.typesafe" % "config" % "1.0.2"

unmanagedResourceDirectories in Compile <+=
    (baseDirectory) { _ / "src" / "main" / "webapp" }