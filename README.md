#STORAGE SYSTEMS COMPARISON

In this project we present a comparison between different storage systems:

- a traditional Relational Database - MySQL
- a NoSQL Database - MongoDB
- a Graph Database - Neo4j

To do so, we use a big dataset containing more than 160M records of commercial flights information in the USA between 1987 and 2015. The raw dataset has been pre-aggregated and formatted accordingly for each of the databases with the use of Spark. In two cases, Spark was also used to import the data to the databases.
This evaluation has been achieved by comparing implementation complexity, integration with Spark, and comparing time performances of running several queries on each of the databases.

<br>You can find a complete [report](https://github.com/federico-fiorini/storage-systems-comparison/report.pdf) and a [poster](https://github.com/federico-fiorini/storage-systems-comparison/poster.pdf) explaining in details implementation and results.

###SOME RESULTS
![image](http://i.imgur.com/jboSpJd.png)

###HOW TO USE
Define the path to your Spark folder of installation as an environment variable named SPARK_HOME. 

	$ export SPARK_HOME=/path-to-folder/spark-1.5.2
	
Run the different script to process the import to MySQL, MongoDB and Neo4j.

	$ ./process_mysql.sh
	$ ./process_mongo.sh
	$ ./process_neo4j.sh

If you want to modify something you need to rebuild the jar with sbt (you need to install it first)
	
	sbt clean package

###RELATED WORK

We used the aggregated and cleaned data imported in Neo4j to apply some clustering algorithms on the resulting graph and detect communities in the network. Using the frequency of the routes as our edge weight, we could find communities of airports well connected between each other.

You can find this related project [here](https://github.com/federico-fiorini/network-communities).