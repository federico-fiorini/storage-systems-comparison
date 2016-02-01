# Remove old database and tmp csv files
rm -rf tmp/neo
rm -rf tmp/airports*
rm -rf tmp/routes*

JARS=""

# Find list of jars to include
for filename in lib/NEO4J_*; do
    JARS=$JARS$filename","
done

JARS=${JARS%?}

# Run preprocess
SPARK_SUBMIT=${SPARK_HOME%/}"/bin/spark-submit --class PreprocessCSVforNeo4j --master local[*] target/scala-2.10/storage-systems-comparison_2.10-1.0.jar"
$SPARK_SUBMIT


# Merge airport csv files
./merge-csv.sh tmp/airports

# Remove duplicates
(head -n 1 tmp/airports.csv && tail -n +2 tmp/airports.csv | sort -u -t',' -k1,1) > tmp/air.csv

rm tmp/airports.csv
mv tmp/air.csv tmp/airports.csv

# Find list of csv files and build neo4j-import command

# Find list of airports files
NODES="--nodes "

for filename in tmp/airports*; do
    NODES=$NODES$filename","
done

NODES=${NODES%?}


# Find list of routes files
REL="--relationships "

for filename in tmp/routes*; do
    REL=$REL$filename","
done

REL=${REL%?}

COMMAND="neo4j-import --into tmp/neo $NODES $REL --skip-duplicate-nodes"

# Import to new neo4j database
$COMMAND

# Remove temporary files
#rm -rf tmp/airports*
#rm -rf tmp/routes*
