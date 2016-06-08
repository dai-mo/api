import Dependencies._
import Common._

lazy val apiProjectName = "org.dcs.api"
lazy val apiProjectID   = "api"
lazy val apiProjectDir   = "."

lazy val api = OsgiProject(apiProjectID, apiProjectName, apiProjectDir).
    settings(libraryDependencies ++= apiDependencies)

