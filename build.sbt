ThisBuild / crossScalaVersions := Seq("2.13.10")
ThisBuild / scalaVersion := crossScalaVersions.value.last

ThisBuild / githubWorkflowPublishTargetBranches := Seq()

enablePlugins(JmhPlugin)

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-effect" % "3.3.12",
  "dev.zio"       %% "zio" % "2.0.3")
