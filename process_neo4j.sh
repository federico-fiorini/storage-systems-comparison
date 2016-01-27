# Remove old database
rm -rf tmp/neo

# Run preprocess
SPARK_SUBMIT=${SPARK_HOME%/}"/bin/spark-submit --class PreprocessCSVforNeo4j --master local[8] target/scala-2.10/storage-systems-comparison_2.10-1.0.jar"
$SPARK_SUBMIT

# Find list of csv files and build neo4j-import command
COMMAND=""
NODE=" --nodes "
REL=" --relationships "

# Find list of airports files
for filename in tmp/airports*; do
    COMMAND=$COMMAND$NODE$filename
done

# Find list of routes files
for filename in tmp/routes*; do
    COMMAND=$COMMAND$REL$filename
done

FINAL="neo4j-import --into tmp/neo"$COMMAND" --skip-duplicate-nodes true"

# Import to new neo4j database
$FINAL
