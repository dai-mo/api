import sbt._

object Dependencies {
  lazy val scVersion          = "2.11.7"

  // Versions
  lazy val dcsCommonsVersion  = "0.2.0"
  lazy val dcsTestVersion     = "0.1.0"
  lazy val avroVersion 			  = "1.8.1"
  lazy val guavaVersion       = "18.0"

  val dcsCommons      = "org.dcs"               % "org.dcs.commons"            % dcsCommonsVersion
  val dcsTest         = "org.dcs"               % "org.dcs.test"               % dcsTestVersion
  val avro            = "org.apache.avro"       % "avro"                       % avroVersion
  val guava           = "com.google.guava"      % "guava"                      % guavaVersion

  // Collect Api Dependencies
  val apiDependencies = Seq(
    dcsCommons      % "provided",
    avro            % "provided",
    guava,
    dcsTest         % "test"
  )
}
