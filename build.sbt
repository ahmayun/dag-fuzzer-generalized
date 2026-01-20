ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.3.0",
  "com.github.jnr" % "jnr-unixsocket" % "0.38.16",
  "org.xerial" % "sqlite-jdbc" % "3.36.0.3",
  "com.github.javaparser" % "javaparser-core" % "3.25.5",
  "com.github.jsqlparser" % "jsqlparser" % "4.7",
  "org.scalameta" %% "scalameta" % "4.10.2",
  "com.typesafe.play" %% "play-json" % "2.9.4",
  "org.yaml" % "snakeyaml" % "2.2",
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "net.bytebuddy" % "byte-buddy" % "1.14.11",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2"
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2"

// JUnit (core testing library)
libraryDependencies += "junit" % "junit" % "4.13.2" % Test
// JUnit Interface to run JUnit tests with SBT
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test
// Enable JUnit test framework
testFrameworks += new TestFramework("com.novocode.junit.JUnitFramework")


lazy val root = (project in file("."))
  .settings(
    name := "DAGFuzzerBetter",
    Compile / scalaSource := sourceDirectory.value / "main" / "scala",
    Test / scalaSource := sourceDirectory.value / "test" / "scala",
  )

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case _ => MergeStrategy.first
}
