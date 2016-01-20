import os
import sys
import urllib
from datetime import timedelta, date

try:
  from pyspark import SparkContext, SparkConf
  from pyspark.sql import SQLContext, Row

  print("Spark Modules Imported")

except ImportError as e:
  print("Error Importing", e)
  sys.exit(1)

def importToMysql(filename):
  lines = sc.textFile(filename)
  arrays = lines.map(lambda l: l.split(",")).filter( lambda l:  "Year" not in l ) # Split values and drop the header
  tuples = arrays.map(lambda a: ((a[0], a[1], a[2], a[16], a[17],),1)).reduceByKey(lambda a, b: a + b) #group with spark
  # Convert to Rows
  flights = tuples.map(
    lambda p: Row(
      year = int(p[0][0]),
      month = int(p[0][1]),
      day = int(p[0][2]),
      origin = p[0][3],
      destination = p[0][4],
      frequency = p[1]
      )
    )

  # # Form the rows to group by with sql
  # flights = arrays.map(
  #   lambda p: Row(
  #     year = int(p[0]),
  #     month = int(p[1]),
  #     day = int(p[2]),
  #     origin = p[16],
  #     destination = p[17]
  #     )
  #   )

  # Infer the schema, and register the DataFrame as a table.
  column_order = ["year", "month", "day", "origin", "destination", "frequency" ]
  flightsDataFrame = sqlContext.createDataFrame(flights)[column_order]
  # schema = sqlContext.createDataFrame(flights)
  # schema.registerTempTable("tempflights")

  # # Group the flights by day and route
  # grouped_with_sql_flights = sqlContext.sql("SELECT year, month, day, origin, destination, count(*) AS frequency FROM tempflights GROUP BY year, month, day, origin, destination")

  # print("GROUPED SQL CLASS" + flightsDataFrame.__class__.__name__)
  # Save Flights to MySQL
  # grouped_with_sql_flights.write.jdbc(
  flightsDataFrame.write.jdbc(
    url = "jdbc:mysql://127.0.0.1:3306", 
    table = "flights.routes", 
    mode = "append", 
    properties = {"user":"root", "password":"root","driver":"com.mysql.jdbc.Driver"}
  )

sc = SparkContext('local')
sqlContext = SQLContext(sc)

for csvFile in os.listdir("dataset"):
  if csvFile.endswith(".csv.bz2"):
    importToMysql("dataset/" + csvFile)

