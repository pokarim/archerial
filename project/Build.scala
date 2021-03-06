import sbt._
import Keys._

object ArcherialBuild extends Build {

    val appName         = "archerial"
    val appVersion      = "0.0.1-SNAPSHOT"

    val appDependencies = Seq(
	  "org.scalaz" %% "scalaz-core" % "6.0.4",
	  "play" %% "play" % "2.0.4",
	  "play" %% "play" % "2.0.4" % "test",
	  "org.specs2" %% "specs2" % "1.12.1" % "test",
	  "com.codahale" %% "jerkson" % "0.5.0"
    )
  val mysettings = Project.defaultSettings ++ Seq(
	libraryDependencies ++= appDependencies,
	resolvers ++= Seq(
      DefaultMavenRepository,
	  "Codahale" at "http://repo.codahale.com",
	  "Typesafe repo" at "http://repo.typesafe.com/typesafe/repo",
	  "snapshots" at 
	  "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at 
	  "http://oss.sonatype.org/content/repositories/releases",
      Resolver.url(
		"Play", 
		url("http://download.playframework.org/ivy-releases/")
	  )(Resolver.ivyStylePatterns)
	))
  lazy val root = Project(id = "Archerial",
                          base = file("."),
                          settings = mysettings)

}
