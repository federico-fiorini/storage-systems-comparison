import os
import sys
import urllib
import re
from datetime import timedelta, date

import MySQLdb
import MySQLdb.cursors
from warnings import filterwarnings

try:
  from pyspark import SparkContext, SparkConf
  from pyspark.sql import SQLContext, Row

  print("Spark Modules Imported")

except ImportError as e:
  print("Error Importing", e)
  sys.exit(1)

# Define parameters
datasetFolder = "dataset"

mysqlHost = "127.0.0.1"
mysqlPort = "3306"
mysqlUser = "root"
mysqlPassword = "root"
mysqlDatabase = "flights"

def connectToMysql():
  # Ignore mysql warnings
  filterwarnings('ignore', category = MySQLdb.Warning)

  # Create connection
  return MySQLdb.connect(
      host = mysqlHost,
      user = mysqlUser,
      passwd = mysqlPassword,
      db = mysqlDatabase,
  )

def initMysql():
  # Connect to MySQL
  conn = connectToMysql()
  query = conn.cursor()

  # Drop table if exists
  sql = """
    DROP TABLE IF EXISTS `%s`.`routes`;
  """ % mysqlDatabase
  query.execute(sql)

  # Create table
  sql = """
    CREATE TABLE `%s`.`routes` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `year` int(11) DEFAULT NULL,
    `month` int(11) DEFAULT NULL,
    `day` int(11) DEFAULT NULL,
    `origin` varchar(10) DEFAULT NULL,
    `destination` varchar(10) DEFAULT NULL,
    `frequency` int(11) DEFAULT NULL,
    PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
  """ % mysqlDatabase
  query.execute(sql)

  conn.close()

def removeCommas(line):
  while re.search('\"[^,"]+(,)[^,"]+\"', line):
    line = line.replace(re.search('\"[^,"]+(,)[^,"]+\"', line).group(0), re.search('\"[^,"]+(,)[^,"]+\"', line).group(0).replace(',',''))
  return line

def removeHeader(idx, iterator):
  return iter(list(iterator)[1:]) if idx == 0 else iterator

def importToMysql(filename):
  # Load file
  lines = sc.textFile(filename)
  arrays = lines.map(lambda l: removeCommas(l).replace('"', '').split(","))

  # Find columns indexes
  header = map(lambda x: x.lower(), arrays.first())
  indexes = {}
  columns = ["year", "month", "day_of_month", "origin", "dest"]

  for columnName in columns:
    indexes[columnName] = header.index(columnName)

  withoutHeader = arrays.mapPartitionsWithIndex(removeHeader)
  tuples = withoutHeader.map( lambda row: ((row[indexes['year']], row[indexes['month']], row[indexes['day_of_month']], row[indexes['origin']], row[indexes['dest']]), 1)).reduceByKey(lambda a, b: a + b) #group with spark

  # Convert to Rows
  flights = tuples.map(
    lambda p: Row(
      id = 0,
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
  column_order = ["id", "year", "month", "day", "origin", "destination", "frequency" ]
  flightsDataFrame = sqlContext.createDataFrame(flights)[column_order]
  
  # schema = sqlContext.createDataFrame(flights)
  # schema.registerTempTable("tempflights")

  # # Group the flights by day and route
  # grouped_with_sql_flights = sqlContext.sql("SELECT year, month, day, origin, destination, count(*) AS frequency FROM tempflights GROUP BY year, month, day, origin, destination")

  # print("GROUPED SQL CLASS" + flightsDataFrame.__class__.__name__)
  # Save Flights to MySQL
  # grouped_with_sql_flights.write.jdbc(
  flightsDataFrame.write.jdbc(
    url = "jdbc:mysql://" + mysqlHost + ":" + mysqlPort, 
    table = mysqlDatabase + ".routes", 
    mode = "append", 
    properties = {"user":mysqlUser, "password":mysqlPassword, "driver":"com.mysql.jdbc.Driver"}
  )

# Init spark context
sc = SparkContext('local')
sqlContext = SQLContext(sc)

# Init mysql table
initMysql()

# Foreach file in the dataset folder: import to mysql
for csvFile in os.listdir(datasetFolder):
  if csvFile.endswith(".csv"):
    importToMysql(datasetFolder + "/" + csvFile)

