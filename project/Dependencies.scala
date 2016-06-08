import sbt._

object Dependencies {
  lazy val scVersion          = "2.11.7"
  lazy val dcsApiVersion      = "1.0.0-SNAPSHOT"

  // Versions
  lazy val dcsCommonsVersion  = "1.0.0-SNAPSHOT"
  lazy val dcsTestVersion   = "1.0.0-SNAPSHOT"

  val dcsCommons      = "org.dcs"               % "org.dcs.commons"            % dcsCommonsVersion
  val dcsTest         = "org.dcs"               % "org.dcs.test"               % dcsTestVersion


  // Collect Api Dependencies
  val apiDependencies = Seq(
    dcsCommons,
    dcsTest         % "test"
  )
}
