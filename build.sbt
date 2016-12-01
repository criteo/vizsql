name := "vizsql"

version := "1.0.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.8"

lazy val root = project.in(file("."))
  .enablePlugins(ScalaJSPlugin)
  .aggregate(vizsqlJS, vizsqlJVM)
  .settings()

lazy val vizsql = crossProject.in(file("."))
  .settings(
    organization := "com.criteo",
    libraryDependencies += "org.scalactic" %%% "scalactic" % "3.0.0",
    libraryDependencies += "org.scalatest" %%% "scalatest" % "3.0.0" % "test"
  )
  .jvmSettings(
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
  )
  .jsSettings(
    libraryDependencies += "org.scala-js" %%% "scala-parser-combinators" % "1.0.2",
    scalaJSModuleKind := ModuleKind.CommonJSModule
  )

lazy val vizsqlJVM = vizsql.jvm
lazy val vizsqlJS = vizsql.js
