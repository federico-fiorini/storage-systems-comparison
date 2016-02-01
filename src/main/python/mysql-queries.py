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
print "================================================"
print "Query #1: Find the most frequent route per month"
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
print "Time: " + str(b-a) + "\n"

# Query 1.b
print "================================================="
print "Query #1.b: Find the most frequent route per year"
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
print "Time: " + str(b-a) + "\n"


# Query 2
print "======================================================================="
print "Query #2: Find the airport with more flights (in and out) per month"
a = datetime.datetime.now().replace(microsecond=0)

sql = """
	SELECT year, month, airport, max(flights) AS tot_flights
	FROM
		(SELECT year, month, airport, sum(monthly_freq) AS flights
		FROM
			(
				(SELECT r.year, r.month, r.origin AS airport, sum(r.frequency) AS monthly_freq
				FROM flights.routes as r
				GROUP BY r.year, r.month, r.origin)
			UNION
				(SELECT r.year, r.month, r.destination AS airport, sum(r.frequency) AS monthly_freq
				FROM flights.routes as r
				GROUP BY r.year, r.month, r.destination)
			) as u
		GROUP BY year, month, airport
		ORDER BY sum(monthly_freq) DESC) as m
	GROUP BY year, month;
	"""
query.execute(sql)
b = datetime.datetime.now().replace(microsecond=0)

table = PrettyTable(["year", "month", "airport", "tot_flights"])
for row in query.fetchall():
	table.add_row(row)
print table
print "Time: " + str(b-a) + "\n"

# Query 2.b
print "======================================================================="
print "Query #2.b: Find the airport with more flights (in and out) per year"
a = datetime.datetime.now().replace(microsecond=0)

sql = """
	SELECT year, airport, max(flights) AS tot_flights
	FROM 
		(SELECT year, airport, sum(yearly_freq) AS flights
		FROM
			(
				(SELECT r.year, r.origin AS airport, sum(r.frequency) AS yearly_freq
				FROM flights.routes as r
				GROUP BY r.year, r.origin)
			UNION
				(SELECT r.year, r.destination AS airport, sum(r.frequency) AS yearly_freq
				FROM flights.routes as r
				GROUP BY r.year, r.destination)
			) as u
		GROUP BY year, airport
		ORDER BY sum(yearly_freq) DESC) as m
	GROUP BY year;
	"""
query.execute(sql)
b = datetime.datetime.now().replace(microsecond=0)

table = PrettyTable(["year", "airport", "tot_flights"])
for row in query.fetchall():
	table.add_row(row)
print table
print "Time: " + str(b-a) + "\n"


# Query 4
print "==============================================================="
print "Query #4: Find the state with more internal flights (per month)"
a = datetime.datetime.now().replace(microsecond=0)

sql = """
	SELECT r1.year, r1.month, r1.state, max(r1.monthly_freq) as monthly_freq
	FROM ( 
		SELECT r.year, r.month, a1.state, sum(r.frequency) as monthly_freq
		FROM flights.routes r
		JOIN flights.airports a1 ON a1.code = r.origin
		JOIN flights.airports a2 ON a2.code = r.destination
		WHERE a1.state = a2.state
		GROUP BY r.year, r.month, a1.state
		ORDER BY sum(r.frequency) DESC ) as r1
	GROUP BY r1.year, r1.month;
	"""
query.execute(sql)
b = datetime.datetime.now().replace(microsecond=0)

table = PrettyTable(["year", "month", "state", "monthly_freq"])
for row in query.fetchall():
	table.add_row(row)
print table
print "Time: " + str(b-a) + "\n"


# Query 4.b
print "================================================================"
print "Query #4.b: Find the state with more internal flights (per year)"
a = datetime.datetime.now().replace(microsecond=0)

sql = """
	SELECT r1.year, r1.state, max(r1.yearly_freq) as yearly_freq
	FROM ( 
		SELECT r.year, a1.state, sum(r.frequency) as yearly_freq
		FROM flights.routes r
		JOIN flights.airports a1 ON a1.code = r.origin
		JOIN flights.airports a2 ON a2.code = r.destination
		WHERE a1.state = a2.state
		GROUP BY r.year, a1.state
		ORDER BY sum(r.frequency) DESC ) as r1
	GROUP BY r1.year;
	"""
query.execute(sql)
b = datetime.datetime.now().replace(microsecond=0)

table = PrettyTable(["year", "state", "yearly_freq"])
for row in query.fetchall():
	table.add_row(row)
print table
print "Time: " + str(b-a) + "\n"


# Query 5
print "================================================================================="
print "Query #5: Find the state with more departure flights to another state (per month)"
a = datetime.datetime.now().replace(microsecond=0)

sql = """
	SELECT r1.year, r1.month, r1.state, max(r1.monthly_freq) as monthly_freq
	FROM ( 
		SELECT r.year, r.month, a1.state, sum(r.frequency) as monthly_freq
		FROM flights.routes r
		JOIN flights.airports a1 ON a1.code = r.origin
		JOIN flights.airports a2 ON a2.code = r.destination
		WHERE a1.state <> a2.state
		GROUP BY r.year, r.month, a1.state
		ORDER BY sum(r.frequency) DESC ) as r1
	GROUP BY r1.year, r1.month;
	"""
query.execute(sql)
b = datetime.datetime.now().replace(microsecond=0)

table = PrettyTable(["year", "month", "state", "monthly_freq"])
for row in query.fetchall():
	table.add_row(row)
print table
print "Time: " + str(b-a) + "\n"


# Query 5.b
print "==================================================================================="
print "Query #5.b: Find the state with more departure flights to another state  (per year)"
a = datetime.datetime.now().replace(microsecond=0)

sql = """
	SELECT r1.year, r1.state, max(r1.yearly_freq) as yearly_freq
	FROM ( 
		SELECT r.year, a1.state, sum(r.frequency) as yearly_freq
		FROM flights.routes r
		JOIN flights.airports a1 ON a1.code = r.origin
		JOIN flights.airports a2 ON a2.code = r.destination
		WHERE a1.state <> a2.state
		GROUP BY r.year, a1.state
		ORDER BY sum(r.frequency) DESC ) as r1
	GROUP BY r1.year;
	"""
query.execute(sql)
b = datetime.datetime.now().replace(microsecond=0)

table = PrettyTable(["year", "state", "yearly_freq"])
for row in query.fetchall():
	table.add_row(row)
print table
print "Time: " + str(b-a) + "\n"


# Query 6
print "================================================================================="
print "Query #6: Find the state with more arrival flights from another state (per month)"
a = datetime.datetime.now().replace(microsecond=0)

sql = """
	SELECT r1.year, r1.month, r1.state, max(r1.monthly_freq) as monthly_freq
	FROM ( 
		SELECT r.year, r.month, a2.state, sum(r.frequency) as monthly_freq
		FROM flights.routes r
		JOIN flights.airports a1 ON a1.code = r.origin
		JOIN flights.airports a2 ON a2.code = r.destination
		WHERE a1.state <> a2.state
		GROUP BY r.year, r.month, a2.state
		ORDER BY sum(r.frequency) DESC ) as r1
	GROUP BY r1.year, r1.month;
	"""
query.execute(sql)
b = datetime.datetime.now().replace(microsecond=0)

table = PrettyTable(["year", "month", "state", "monthly_freq"])
for row in query.fetchall():
	table.add_row(row)
print table
print "Time: " + str(b-a) + "\n"

# Query 6.b
print "=================================================================================="
print "Query #6.b: Find the state with more arrival flights from another state (per year)"
a = datetime.datetime.now().replace(microsecond=0)

sql = """
	SELECT r1.year, r1.state, max(r1.yearly_freq) as yearly_freq
	FROM ( 
		SELECT r.year, a2.state, sum(r.frequency) as yearly_freq
		FROM flights.routes r
		JOIN flights.airports a1 ON a1.code = r.origin
		JOIN flights.airports a2 ON a2.code = r.destination
		WHERE a1.state <> a2.state
		GROUP BY r.year, a2.state
		ORDER BY sum(r.frequency) DESC ) as r1
	GROUP BY r1.year;
	"""
query.execute(sql)
b = datetime.datetime.now().replace(microsecond=0)

table = PrettyTable(["year", "state", "yearly_freq"])
for row in query.fetchall():
	table.add_row(row)
print table
print "Time: " + str(b-a) + "\n"

