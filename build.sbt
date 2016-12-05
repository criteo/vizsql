name := "vizsql"

scalaVersion in ThisBuild := "2.11.8"

lazy val root = project.in(file("."))
  .enablePlugins(ScalaJSPlugin)
  .aggregate(vizsqlJS, vizsqlJVM)
  .settings()

lazy val vizsql = crossProject.in(file("."))
  .settings(
    libraryDependencies += "org.scalactic" %%% "scalactic" % "3.0.0",
    libraryDependencies += "org.scalatest" %%% "scalatest" % "3.0.0" % "test"
  )
  .jvmSettings(
    organization := "com.criteo",
    version := "1.0.0",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
    credentials += Credentials(
      "Sonatype Nexus Repository Manager",
      "oss.sonatype.org",
      "criteo-oss",
      sys.env.getOrElse("SONATYPE_PASSWORD", "")
    )
  )
  .jsSettings(
    libraryDependencies += "org.scala-js" %%% "scala-parser-combinators" % "1.0.2",
    scalaJSModuleKind := ModuleKind.CommonJSModule
  )

lazy val vizsqlJVM = vizsql.jvm
lazy val vizsqlJS = vizsql.js

// To sync with Maven central, you need to supply the following information:
pomExtra in Global := {
  <url>https://github.com/criteo/vizsql</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/criteo/vizsql.git</connection>
      <developerConnection>scm:git:git@github.com:criteo/vizsql.git</developerConnection>
      <url>github.com/criteo/vizsql</url>
    </scm>
    <developers>
      <developer>
        <name>Guillaume Bort</name>
        <email>g.bort@criteo.com</email>
        <url>https://github.com/guillaumebort</url>
        <organization>Criteo</organization>
        <organizationUrl>http://www.criteo.com</organizationUrl>
      </developer>
    </developers>
}

pgpPassphrase := sys.env.get("SONATYPE_PASSWORD").map(_.toArray)
pgpSecretRing := file(".travis/secring.gpg")
pgpPublicRing := file(".travis/pubring.gpg")
usePgpKeyHex("755d525885532e9e")
