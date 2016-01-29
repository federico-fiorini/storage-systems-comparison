import datetime

import MySQLdb
import MySQLdb.cursors
from warnings import filterwarnings

from prettytable import PrettyTable

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

# Connect to mysql
conn = connectToMysql()
query = conn.cursor()

# Query 1
print "======================================================================="
print "Query #1: Find the most frequent route per month (with only airport ID)"
a = datetime.datetime.now().replace(microsecond=0)

sql = """
	SELECT r2.year, r2.month, r2.origin, r2.destination, max(r2.monthly_freq) AS max_monthly_freq
	FROM (SELECT r.year, r.month, r.origin, r.destination, sum(r.frequency) AS monthly_freq
		FROM flights.routes as r
		GROUP BY r.year, r.month, r.origin, r.destination
		ORDER BY sum(r.frequency) DESC) as r2
	GROUP BY r2.year, r2.month;
	"""
query.execute(sql)
b = datetime.datetime.now().replace(microsecond=0)

table = PrettyTable(["year", "month", "origin", "destination", "max_monthly_freq"])
for row in query.fetchall():
	table.add_row(row)
print table
print "Time to read: " + str(b-a) + "\n"

# Query 1.b
print "=========================================================================================="
print "Query #1.b: Find the most frequent route per month with airports information (city, state)"
a = datetime.datetime.now().replace(microsecond=0)

sql = """
	SELECT r2.year, r2.month, r2.origin, a1.city AS origin_city, a1.state  AS origin_state, r2.destination, a2.city AS destination_city, a2.state  AS destination_state, max(r2.monthly_freq) AS max_monthly_freq
	FROM (SELECT r.year, r.month, r.origin, r.destination, sum(r.frequency) AS monthly_freq
		FROM flights.routes as r
		GROUP BY r.year, r.month, r.origin, r.destination
		ORDER BY sum(r.frequency) DESC) as r2
	JOIN flights.airports as a1
		ON r2.origin = a1.code
	JOIN flights.airports as a2
		ON r2.destination = a2.code
	GROUP BY r2.year, r2.month;
	"""
query.execute(sql)
b = datetime.datetime.now().replace(microsecond=0)

table = PrettyTable(["year", "month", "origin", "origin_city", "origin_state", "destination", "destination_city", "destination_state", "max_monthly_freq"])
for row in query.fetchall():
	table.add_row(row)
print table
print "Time to read: " + str(b-a) + "\n"


# Query 2
print "======================================================================="
print "Query #2: Find the most frequent route per year (with only airport ID)"
a = datetime.datetime.now().replace(microsecond=0)

sql = """
	SELECT r2.year, r2.origin, r2.destination, max(r2.yearly_freq) AS max_yearly_freq
	FROM (SELECT r.year, r.origin, r.destination, sum(r.frequency) AS yearly_freq
		FROM flights.routes as r
		GROUP BY r.year, r.origin, r.destination
		ORDER BY sum(r.frequency) DESC) as r2
	GROUP BY r2.year;
	"""
query.execute(sql)
b = datetime.datetime.now().replace(microsecond=0)

table = PrettyTable(["year", "origin", "destination", "max_yearly_freq"])
for row in query.fetchall():
	table.add_row(row)
print table
print "Time to read: " + str(b-a) + "\n"

# Query 2.b
print "=========================================================================================="
print "Query #2.b: Find the most frequent route per year with airports information (city, state)"
a = datetime.datetime.now().replace(microsecond=0)

sql = """
	SELECT r2.year, r2.origin, a1.city AS origin_city, a1.state  AS origin_state, r2.destination, a2.city AS destnation_city, a2.state  AS destnation_state, max(r2.yearly_freq) AS max_yearly_freq
	FROM (SELECT r.year, r.origin, r.destination, sum(r.frequency) AS yearly_freq
		FROM flights.routes as r
		GROUP BY r.year, r.origin, r.destination
		ORDER BY sum(r.frequency) DESC) as r2
	JOIN flights.airports as a1
		ON r2.origin = a1.code
	JOIN flights.airports as a2
		ON r2.destination = a2.code
	GROUP BY r2.year;
	"""
query.execute(sql)
b = datetime.datetime.now().replace(microsecond=0)

table = PrettyTable(["year", "origin", "origin_city", "origin_state", "destination", "destination_city", "destination_state", "max_yearly_freq"])
for row in query.fetchall():
	table.add_row(row)
print table
print "Time to read: " + str(b-a) + "\n"
