
lazy val `amazon` = (project in file(".")).
  settings(
    name := "amz-de",
    organization:= "com.beamsuntory.amz.de",
    version := "1.0.0-00-SNAPSHOT",
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("myPackage.AmzDEMain")
  )

// disable using the Scala version in output paths and artifacts
crossPaths := false

//option to avoid warnings
updateOptions := updateOptions.value.withLatestSnapshots(false)

val sparkVersion = "2.2.0"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.typesafe" % "config" % "1.3.1",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  //"junit" % "junit" % "4.12" % "test",
  "com.beamsuntory.bgc.commons" % "commons" % "1.0.0-04",
  "org.scalatest" %% "scalatest" % "2.2.2" % Test,
  "com.beamsuntory.thebar.gl" % "edw" % "1.0.0-00"
)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

compileScalastyle := scalastyle.in(Compile).toTask("").value

(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

sources in (Compile,doc) := Seq.empty
publishArtifact in (Compile, packageDoc) := false

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

resolvers += "Nexus-Snapshot" at "http://104.154.138.29:8081/repository/maven-snapshots/"
resolvers += "Nexus-Releases" at "http://104.154.138.29:8081/repository/maven-releases/"

publishTo := {

  val nexus = "http://104.154.138.29:8081/"

  if (version.value.trim.endsWith("SNAPSHOT")) {
    Some("snapshots" at nexus + "repository/maven-snapshots/")
  } else {
    Some("releases" at nexus + "repository/maven-releases/")
  }

}