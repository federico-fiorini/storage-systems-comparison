# Find list of csv files and build neo4j-import command
COMMAND=""

# Find list of airports files
for filename in lib/*; do
    COMMAND=$COMMAND$filename","
done

COMMAND=${COMMAND%?}
FINAL=${SPARK_HOME%/}"/bin/spark-submit --driver-memory 5g --class ImportToMongo --master local[8] --jars "$COMMAND" target/scala-2.10/bigdata-project_2.10-1.0.jar"

# Import to new neo4j database
$FINAL
