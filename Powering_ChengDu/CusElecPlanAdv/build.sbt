lazy val root = (project in file(".")).
    settings(
        name := "CusEpPlA",
        version := "2.0",
        scalaVersion := "2.11.8",
        libraryDependencies ++= Seq(
            "org.apache.spark" % "spark-core_2.11" % "2.1.0",
        ) 
    )
