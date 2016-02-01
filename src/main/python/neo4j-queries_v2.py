import datetime
from py2neo import Graph, Node, Relationship

graph = Graph()

def runQueryAndGetTime(query, n=10):
  totalTime = datetime.timedelta(0)
  results = None

  for i in range(n):
    a = datetime.datetime.now()
    results = graph.cypher.execute(query)
    b = datetime.datetime.now()
    totalTime += b-a

  return (results, totalTime/n)

print "===================================================================================="
print "Version 2: aggregate in monthly and yearly frequency in order to have faster queries"

print "=============================="
print "Aggregate in monthly frequency"
print "------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH (a:Airport)-[d:DAILY_FLIGHTS]->(b:Airport)
	WITH a, b, d.year AS year, d.month AS month, sum(toInt(d.frequency)) AS monthly_freq
	CREATE (a)-[m:MONTHLY_FLIGHTS { year : year, month : month, frequency : monthly_freq }]->(b)
	""", n=1
)

print "Time: " + str(time)

print "============================="
print "Aggregate in yearly frequency"
print "-----------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH (a:Airport)-[d:MONTHLY_FLIGHTS]->(b:Airport)
	WITH a, b, d.year AS year, sum(toInt(d.frequency)) AS yearly_freq
	CREATE (a)-[m:YEARLY_FLIGHTS { year : year, frequency : yearly_freq }]->(b)
	""", n=1
)

print "Time: " + str(time)

# Query 1
print "================================================"
print "Query #1: Find the most frequent route per month"
print "------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH ()-[n:MONTHLY_FLIGHTS]->()
	WITH n.year AS year, n.month AS month, max(n.frequency) AS max_month_freq
	MATCH (a:Airport)-[n:MONTHLY_FLIGHTS]->(b:Airport)
	WHERE n.frequency=max_month_freq
	RETURN year, month, a.code AS origin, a.city AS origin_city, a.state AS origin_state, b.code AS dest, b.city AS dest_city, b.state AS dest_state, n.frequency AS frequency
	""")

print results
print "Time: " + str(time) + "\n"

# Query 1.b
print "================================================="
print "Query #1.b: Find the most frequent route per year"
print "-------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH ()-[n:YEARLY_FLIGHTS]->()
	WITH n.year AS year, max(n.frequency) AS max_year_freq
	MATCH (a:Airport)-[n:YEARLY_FLIGHTS]->(b:Airport)
	WHERE n.frequency=max_year_freq
	RETURN year, a.code AS origin, a.city AS origin_city, a.state AS origin_state, b.code AS dest, b.city AS dest_city, b.state AS dest_state, n.frequency AS frequency
	""")

print results
print "Time: " + str(time) + "\n"

# Query 2
print "==================================================================="
print "Query #2: Find the airport with more flights (in and out) per month"
print "-------------------------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH (a:Airport)-[n:MONTHLY_FLIGHTS]-()
	WITH a.code AS code, n.year AS year, n.month AS month, sum(n.frequency) AS flights
	ORDER BY flights DESC
	RETURN year, month, collect(code)[0] AS airport, max(flights) AS tot_flights
	""")

print results
print "Time: " + str(time) + "\n"


# Query 2.b
print "===================================================================="
print "Query #2.b: Find the airport with more flights (in and out) per year"
print "--------------------------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH (a:Airport)-[n:YEARLY_FLIGHTS]-()
	WITH a.code AS code, n.year AS year, sum(n.frequency) AS flights
	ORDER BY flights DESC
	RETURN year, collect(code)[0] AS airport, max(flights) AS tot_flights
	""")

print results
print "Time: " + str(time) + "\n"


# Query 4
print "==================================================================="
print "Query #4: Find the state with more internal flights (per month)"
print "-------------------------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH (a:Airport)-[n:MONTHLY_FLIGHTS]->(b:Airport)
	WHERE a.state = b.state
	WITH n.year AS year, n.month AS month, a.state AS state, sum(n.frequency) AS flights
	ORDER BY flights DESC
	RETURN year, month, collect(state)[0] AS state, max(flights) AS tot_flights
	""")

print results
print "Time: " + str(time) + "\n"


# Query 4.b
print "==================================================================="
print "Query #4.b: Find the state with more internal flights (per year)"
print "-------------------------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH (a:Airport)-[n:YEARLY_FLIGHTS]->(b:Airport)
	WHERE a.state = b.state
	WITH n.year AS year, a.state AS state, sum(n.frequency) AS flights
	ORDER BY flights DESC
	RETURN year, collect(state)[0] AS state, max(flights) AS tot_flights
	""")

print results
print "Time: " + str(time) + "\n"


# Query 5
print "========================================================================="
print "Query #5: Find the state with more external departure flights (per month)"
print "-------------------------------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH (a:Airport)-[n:MONTHLY_FLIGHTS]->(b:Airport)
	WHERE a.state <> b.state
	WITH n.year AS year, n.month AS month, a.state AS state, sum(toInt(n.frequency)) AS flights
	ORDER BY flights DESC
	RETURN year, month, collect(state)[0] AS state, max(flights) AS tot_flights
	""")

print results
print "Time: " + str(time) + "\n"


# Query 5.b
print "=========================================================================="
print "Query #5.b: Find the state with more external departure flights (per year)"
print "--------------------------------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH (a:Airport)-[n:YEARLY_FLIGHTS]->(b:Airport)
	WHERE a.state <> b.state
	WITH n.year AS year, a.state AS state, sum(toInt(n.frequency)) AS flights
	ORDER BY flights DESC
	RETURN year, collect(state)[0] AS state, max(flights) AS tot_flights
	""")

print results
print "Time: " + str(time) + "\n"


# Query 6
print "======================================================================="
print "Query #6: Find the state with more external arrival flights (per month)"
print "-----------------------------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH (a:Airport)<-[n:MONTHLY_FLIGHTS]-(b:Airport)
	WHERE a.state <> b.state
	WITH n.year AS year, n.month AS month, a.state AS state, sum(toInt(n.frequency)) AS flights
	ORDER BY flights DESC
	RETURN year, month, collect(state)[0] AS state, max(flights) AS tot_flights
	""")

print results
print "Time: " + str(time) + "\n"


# Query 6.b
print "========================================================================"
print "Query #6.b: Find the state with more external arrival flights (per year)"
print "------------------------------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH (a:Airport)<-[n:YEARLY_FLIGHTS]-(b:Airport)
	WHERE a.state <> b.state
	WITH n.year AS year, a.state AS state, sum(toInt(n.frequency)) AS flights
	ORDER BY flights DESC
	RETURN year, collect(state)[0] AS state, max(flights) AS tot_flights
	""")

print results
print "Time: " + str(time) + "\n"
