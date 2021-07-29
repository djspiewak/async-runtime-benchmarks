ThisBuild / crossScalaVersions := Seq("2.13.6")
ThisBuild / scalaVersion := crossScalaVersions.value.last

ThisBuild / githubWorkflowPublishTargetBranches := Seq()

enablePlugins(JmhPlugin)

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-effect" % "3.2.0",
  "dev.zio"       %% "zio" % "2.0.0-M1")
