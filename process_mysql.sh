SPARK_SUBMIT=${SPARK_HOME%/}"/bin/spark-submit --master local[8] --jars lib/jars-mysql-connector-java-5.1.6.jar src/main/python/spark-mysql.py"
$SPARK_SUBMIT
