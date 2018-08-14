import sbt._

object Dependencies {
  lazy val scVersion          = "2.11.7"

  // Versions
  lazy val dcsCommonsVersion  = "0.4.0-SNAPSHOT"
  lazy val avroVersion 			  = "1.8.1"
  lazy val guavaVersion       = "18.0"
  lazy val scalaTestVersion 	= "3.0.0"
  lazy val mockitoVersion     = "1.10.19"

  val dcsCommons      = "org.dcs"               % "org.dcs.commons"            % dcsCommonsVersion  
  val avro            = "org.apache.avro"       % "avro"                       % avroVersion
  val guava           = "com.google.guava"      % "guava"                      % guavaVersion
  val scalaTest       = "org.scalatest"         %% "scalatest"                 % scalaTestVersion
  val mockitoAll      = "org.mockito"                      % "mockito-all"                        % mockitoVersion

  // Collect Api Dependencies
  val apiDependencies = Seq(
    dcsCommons      % "provided",
    avro            % "provided",
    guava,
    scalaTest       % "test",
    mockitoAll      % "test"
  )
}
