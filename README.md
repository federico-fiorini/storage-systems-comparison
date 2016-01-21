#STORAGE SYSTEMS COMPARISON

###HOW TO USE
Define the path to your Spark folder of installation as an environment variable named SPARK_HOME. 

	$ export SPARK_HOME=/path-to-folder/spark-1.5.2
	
Run the different script to process the import to MySQL, MongoDB and Neo4j.

	$ ./process_mysql.sh
	$ ./process_mongo.sh
	$ ./process_neo4j.sh

If you want to modify something you need to rebuild the jar with sbt (you need to install it first)
	
	sbt clean package

