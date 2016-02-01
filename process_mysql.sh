JARS=""

# Find list of jars to include
for filename in lib/MYSQL_*; do
    JARS=$JARS$filename","
done

SPARK_SUBMIT=${SPARK_HOME%/}"/bin/spark-submit --master local[*] --jars "$JARS" src/main/python/spark-mysql.py"
$SPARK_SUBMIT
