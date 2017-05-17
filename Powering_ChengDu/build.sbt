lazy val root = (project in file(".")).
    settings(
        name := "PoweringChengDu",
        version := "1.2",
        scalaVersion := "2.11.8",
        libraryDependencies ++= Seq(
            "org.apache.spark" % "spark-core_2.11" % "2.1.0",
            "org.apache.spark" % "spark-mllib_2.11" % "2.1.0",
            "org.apache.spark" % "spark-sql_2.11" % "2.1.0") 
    )
