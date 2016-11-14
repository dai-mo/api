import sbt._

object Dependencies {
  lazy val scVersion          = "2.11.7"

  // Versions
  lazy val dcsCommonsVersion  = "0.2.0-SNAPSHOT"
  lazy val dcsTestVersion     = "0.1.0"
  lazy val avroVersion 			= "1.8.1"

  val dcsCommons      = "org.dcs"               % "org.dcs.commons"            % dcsCommonsVersion
  val dcsTest         = "org.dcs"               % "org.dcs.test"               % dcsTestVersion
  val avro            = "org.apache.avro"       % "avro"                       % avroVersion


  // Collect Api Dependencies
  val apiDependencies = Seq(
    dcsCommons      % "provided",
    avro            % "provided",
    dcsTest         % "test"
  )
}
