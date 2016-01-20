# Remove old database
rm -rf tmp/neo

# build and run preprocess
sbt clean package
/Users/Federico/sources/spark-1.5.2/bin/spark-submit --class PreprocessCSV --master local[8] target/scala-2.10/bigdata-project_2.10-1.0.jar

# Import to new neo4j database
neo4j-import --into tmp/neo --nodes tmp/airports.csv --relationships tmp/routes.csv
