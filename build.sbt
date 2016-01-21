name := "storage-systems-comparison"

version := "1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.4.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.4.1"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.1.0"

libraryDependencies += "net.sf.opencsv" % "opencsv" % "2.3"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.5.2"

addCommandAlias("generate-project",
  ";update-classifiers;update-sbt-classifiers;gen-idea sbt-classifiers")
