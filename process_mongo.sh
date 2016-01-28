JARS=""

# Find list of jars to include
for filename in lib/MONGO_*; do
    JARS=$JARS$filename","
done

JARS=${JARS%?}
COMMAND=${SPARK_HOME%/}"/bin/spark-submit --driver-memory 5g --class ImportToMongo --master local[8] --jars "$JARS" target/scala-2.10/storage-systems-comparison_2.10-1.0.jar"

# Import to new mongo database
$COMMAND
