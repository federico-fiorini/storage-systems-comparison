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

  # Drop routes table if exists
  sql = """
    DROP TABLE IF EXISTS `%s`.`routes`;
  """ % mysqlDatabase
  query.execute(sql)

  # Drop airports table if exists
  sql = """
    DROP TABLE IF EXISTS `%s`.`airports`;
  """ % mysqlDatabase
  query.execute(sql)

  # Create table
  sql = """
    CREATE TABLE `%s`.`routes` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `origin` varchar(3) DEFAULT NULL,
    `destination` varchar(3) DEFAULT NULL,
    `year` int(11) DEFAULT NULL,
    `month` int(11) DEFAULT NULL,
    `day` int(11) DEFAULT NULL,
    `frequency` int(11) DEFAULT NULL,
    PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  CREATE TABLE `%s`.`airports` (
    `code` varchar(3) DEFAULT NULL,
    `city` varchar(55) DEFAULT NULL,
    `state` varchar(55) DEFAULT NULL,
    PRIMARY KEY (`code`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
  """ % ( mysqlDatabase, mysqlDatabase )
  query.execute(sql)

  conn.close()

def removeCommas(line):
  while re.search('\"[^,"]+(,)[^,"]+\"', line):
    line = line.replace(re.search('\"[^,"]+(,)[^,"]+\"', line).group(0), re.search('\"[^,"]+(,)[^,"]+\"', line).group(0).replace(',',''))
  return line

def removeHeader(idx, iterator):
  return iter(list(iterator)[1:]) if idx == 0 else iterator

# Yields the origin and destination airports as 2 separate tuples
def getAirportsFromFlight(flightRow, airportIndexes):
  for indexTuple in airportIndexes:
    yield (flightRow[indexTuple[0]], flightRow[indexTuple[1]], flightRow[indexTuple[2]])

def importToMysql(filename):
  # Load file
  lines = sc.textFile(filename)
  arrays = lines.map(lambda l: removeCommas(l).replace('"', '').split(","))

  # Find columns indexes
  header = map(lambda x: x.lower(), arrays.first())
  indexes = {}
  columns = ["year", "month", "day_of_month", "origin", "origin_city_name", "origin_state_nm", "dest", "dest_city_name", "dest_state_nm"]

  for columnName in columns:
    indexes[columnName] = header.index(columnName)

  withoutHeader = arrays.mapPartitionsWithIndex(removeHeader)
  flight_tuples = withoutHeader.map( lambda row: ((row[indexes['origin']], row[indexes['dest']], row[indexes['year']], row[indexes['month']], row[indexes['day_of_month']]), 1)).reduceByKey(lambda a, b: a + b) #group with spark

  # Convert to Rows
  flights = flight_tuples.map(
    lambda p: Row(
      id = 0,
      origin = p[0][0],
      destination = p[0][1],
      year = int(p[0][2]),
      month = int(p[0][3]),
      day = int(p[0][4]),
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
  column_order = ["id", "origin", "destination", "year", "month", "day", "frequency"]
  flightsDataFrame = sqlContext.createDataFrame(flights)[column_order]
  
  # flightsDataFrame.registerTempTable("tempflights")
  # # Group the flights by day and route
  # grouped_with_sql_flights = sqlContext.sql("SELECT year, month, day, origin, destination, count(*) AS frequency FROM tempflights GROUP BY year, month, day, origin, destination")

  # Save Flights to MySQL
  # grouped_with_sql_flights.write.jdbc(
  flightsDataFrame.write.jdbc(
    url = "jdbc:mysql://" + mysqlHost + ":" + mysqlPort, 
    table = mysqlDatabase + ".routes", 
    mode = "append", 
    properties = {"user":mysqlUser, "password":mysqlPassword, "driver":"com.mysql.jdbc.Driver"}
  )

  airportIndexes = [(indexes["origin"], indexes["origin_city_name"], indexes["origin_state_nm"]),
                    (indexes["dest"], indexes["dest_city_name"], indexes["dest_state_nm"])]

  # Map and Reduce to get list of all airports
  airport_tuples = withoutHeader.flatMap( lambda row: getAirportsFromFlight(row, airportIndexes) ).distinct()
  airports = airport_tuples.map( 
    lambda row: Row(
        code = row[0],
        city = row[1],
        state = row[2]
      )
    )

  # Infer the schema, and register the DataFrame as a table.
  airports_column_order = ["code", "city", "state"]
  airportsDataFrame = sqlContext.createDataFrame(airports)[airports_column_order]

  airportsDataFrame.write.jdbc(
    url = "jdbc:mysql://" + mysqlHost + ":" + mysqlPort, 
    table = mysqlDatabase + ".airports", 
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

