

import Dependencies._
import com.typesafe.sbt.osgi.SbtOsgi
import com.typesafe.sbt.osgi.SbtOsgi.autoImport._
import sbt.Keys._
import sbt._

object Common {

  lazy val UNIT = config("unit") extend(Test)
  lazy val IT = config("it") extend(Test)

  lazy val commonSettings = Seq(
    organization := "org.dcs",
    scalaVersion := scVersion,
    crossPaths := false,
    checksums in update := Nil,
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
    javacOptions in doc := Seq("-source", "1.8")
  )

  def BaseProject(projectID: String,
                  projectName: String,
                  projectFileName: String) =
    Project(projectID, file(projectFileName)).
      settings(commonSettings: _*).
      settings(
        name := projectName,
        moduleName := projectName).
      configs(IT).
      settings(inConfig(IT)(Defaults.testTasks): _*).
      settings(testOptions in IT := Seq(Tests.Argument("-n", "IT"))).
      configs(UNIT).
      settings(inConfig(UNIT)(Defaults.testTasks): _*).
      settings(testOptions in UNIT := Seq(
        Tests.Argument("-l", "IT"),
        Tests.Argument("-l", "E2E"))
      )


  lazy val paxCdiCapabilities = "org.ops4j.pax.cdi.extension;" +
    "filter:=\"(&(extension=pax-cdi-extension)(version>=0.12.0)(!(version>=1.0.0)))\"," +
    "osgi.extender;" +
    "filter:=\"(osgi.extender=pax.cdi)\"," +
    "org.ops4j.pax.cdi.extension;" +
    "filter:=\"(extension=pax-cdi-extension)\""

  def OsgiProject(projectID: String,
                  projectName: String,
                  projectFileName: String) =
    BaseProject(projectID, projectName, projectFileName).
      enablePlugins(SbtOsgi).
      settings(
        name := projectName,
        OsgiKeys.bundleSymbolicName := projectName,
        OsgiKeys.exportPackage := Seq(name.value + ".*"),
        OsgiKeys.importPackage := Seq("*"),
        OsgiKeys.requireCapability := paxCdiCapabilities,
        moduleName := name.value).
      settings(osgiSettings: _*)
}
