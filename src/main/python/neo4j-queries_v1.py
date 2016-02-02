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

print "================================================"
print "Version 1: run all queries using daily frequency"

# # Query 1
# print "================================================"
# print "Query #1: Find the most frequent route per month"
# print "------------------------------------------------"

# (results, time) = runQueryAndGetTime("""
# 	MATCH (a:Airport)-[n:DAILY_FLIGHTS]->(b:Airport)
# 	WITH a, b, n.year AS year, n.month AS month, sum(toInt(n.frequency)) AS monthly_freq
# 	ORDER BY monthly_freq DESC
# 	WITH year, month, collect(a)[0] AS a, collect(b)[0] AS b, max(monthly_freq) AS frequency
# 	RETURN year, month, a.code AS origin, a.city AS origin_city, a.state AS origin_state, b.code AS dest, b.city AS dest_city, b.state AS dest_state, frequency
# 	""")

# print results
# print "Time: " + str(time) + "\n"

# # Query 1.b
# print "================================================="
# print "Query #1.b: Find the most frequent route per year"
# print "-------------------------------------------------"

# (results, time) = runQueryAndGetTime("""
# 	MATCH (a:Airport)-[n:DAILY_FLIGHTS]->(b:Airport)
# 	WITH a, b, n.year AS year, sum(toInt(n.frequency)) AS yearly_freq
# 	ORDER BY yearly_freq DESC
# 	WITH year, collect(a)[0] AS a, collect(b)[0] AS b, max(yearly_freq) AS frequency
# 	RETURN year, a.code AS origin, a.city AS origin_city, a.state AS origin_state, b.code AS dest, b.city AS dest_city, b.state AS dest_state, frequency
# 	""")

# print results
# print "Time: " + str(time) + "\n"

# # Query 2
# print "==================================================================="
# print "Query #2: Find the airport with more flights (in and out) per month"
# print "-------------------------------------------------------------------"

# (results, time) = runQueryAndGetTime("""
# 	MATCH (a:Airport)-[n:DAILY_FLIGHTS]-()
# 	WITH a.code AS code, n.year AS year, n.month AS month, sum(toInt(n.frequency)) AS flights
# 	ORDER BY flights DESC
# 	RETURN year, month, collect(code)[0] AS airport, max(flights) AS tot_flights
# 	""")

# print results
# print "Time: " + str(time) + "\n"


# # Query 2.b
# print "===================================================================="
# print "Query #2.b: Find the airport with more flights (in and out) per year"
# print "--------------------------------------------------------------------"

# (results, time) = runQueryAndGetTime("""
# 	MATCH (a:Airport)-[n:DAILY_FLIGHTS]-()
# 	WITH a.code AS code, n.year AS year, sum(toInt(n.frequency)) AS flights
# 	ORDER BY flights DESC
# 	RETURN year, collect(code)[0] AS airport, max(flights) AS tot_flights
# 	""")

# print results
# print "Time: " + str(time) + "\n"


# Query 3
print "========================================================================================"
print "Query #3: Given a departure date find all the shortest paths from airport A to airport B"
print "----------------------------------------------------------------------------------------"

year = "2011"
month = "6"
day = "20"
departure = "RAP"
arrival = "XNA"

print "Route: %s -> %s" % (departure, arrival)
print "Departure: %s/%s/%s" % (day, month, year)
print "----------------------------------------------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH p = allShortestPaths((a:Airport { code:"%s"})-[:DAILY_FLIGHTS*..10]->(b:Airport { code:"%s"}))
	WHERE ALL(r in rels(p) WHERE r.year="%s" AND r.month="%s" AND r.day="%s")
	RETURN p AS shortest_path
	""" % (departure, arrival, year, month, day))

print results
print "Time: " + str(time) + "\n"

exit()
# Query 4
print "==============================================================="
print "Query #4: Find the state with more internal flights (per month)"
print "---------------------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH (a:Airport)-[n:DAILY_FLIGHTS]->(b:Airport)
	WHERE a.state = b.state
	WITH n.year AS year, n.month AS month, a.state AS state, sum(toInt(n.frequency)) AS flights
	ORDER BY flights DESC
	RETURN year, month, collect(state)[0] AS state, max(flights) AS tot_flights
	""")

print results
print "Time: " + str(time) + "\n"


# Query 4.b
print "================================================================"
print "Query #4.b: Find the state with more internal flights (per year)"
print "----------------------------------------------------------------"

(results, time) = runQueryAndGetTime("""
	MATCH (a:Airport)-[n:DAILY_FLIGHTS]->(b:Airport)
	WHERE a.state = b.state
	WITH n.year AS year, a.state AS state, sum(toInt(n.frequency)) AS flights
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
	MATCH (a:Airport)-[n:DAILY_FLIGHTS]->(b:Airport)
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
	MATCH (a:Airport)-[n:DAILY_FLIGHTS]->(b:Airport)
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
	MATCH (a:Airport)<-[n:DAILY_FLIGHTS]-(b:Airport)
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
	MATCH (a:Airport)<-[n:DAILY_FLIGHTS]-(b:Airport)
	WHERE a.state <> b.state
	WITH n.year AS year, a.state AS state, sum(toInt(n.frequency)) AS flights
	ORDER BY flights DESC
	RETURN year, collect(state)[0] AS state, max(flights) AS tot_flights
	""")

print results
print "Time: " + str(time) + "\n"



