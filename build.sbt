import sbtassembly.Plugin._

import AssemblyKeys._

scalaVersion := "2.9.3"

version := "0.0.2-SNAPSHOT"

resolvers += "Sonatype release" at "https://oss.sonatype.org/content/repositories/releases"

resolvers += "Akka repo" at "http://repo.akka.io/releases/"

resolvers += "Spray repo" at "http://repo.spray.cc"

libraryDependencies += "org.spark-project" %% "spark-core" % "0.7.3"

libraryDependencies += "org.spark-project" %% "spark-streaming" % "0.7.3"

libraryDependencies += "com.typesafe" % "config" % "1.0.2"

unmanagedResourceDirectories in Compile <+=
    (baseDirectory) { _ / "src" / "main" / "webapp" }

assemblySettings

jarName in assembly := "p2.jar"

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (current) =>
  {
    // Hack together some dependency nonsenses into a fatjar.
    case s: String if s.startsWith("javax/servlet") => MergeStrategy.first
    case s: String if s.startsWith("org/jboss/netty/") => MergeStrategy.first
    case s: String if s.startsWith("META-INF/jboss-beans.xml") => MergeStrategy.first
    case s: String if s.startsWith("plugin.properties") => MergeStrategy.concat
    case s: String if s.startsWith("org/apache/jasper") => MergeStrategy.first
    case s: String if s.startsWith("org/apache/commons/beanutils") => MergeStrategy.first
    case s: String if s.startsWith("org/apache/commons/collections") => MergeStrategy.first
    case s: String if s.endsWith("about.html") => MergeStrategy.discard
    case x => current(x)
  }
}

mainClass in assembly := Some("be.bigdata.p2.P2")