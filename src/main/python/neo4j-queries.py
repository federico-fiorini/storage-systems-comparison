import datetime
from py2neo import Graph, Node, Relationship

graph = Graph()

# Query 1
print "================================================"
print "Query #1: Find the most frequent route per month"
print "------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)
graph.cypher.execute("""
	MATCH (a:Airport)-[d:DAILY_FLIGHTS]->(b:Airport)
	WITH a, b, d.year AS year, d.month AS month, sum(toInt(d.frequency)) AS monthly_freq
	CREATE (a)-[m:MONTHLY_FLIGHT { year : year, month : month, frequency : monthly_freq }]->(b)
	"""
)

b = datetime.datetime.now().replace(microsecond=0)
print "Time to aggregate in monthly frequency: " + str(b-a)
print "------------------------------------------------"

print graph.cypher.execute("""
	MATCH ()-[n:MONTHLY_FLIGHTS]->()
	WITH n.year AS year, n.month AS month, max(n.frequency) AS max_month_freq
	MATCH (a:Airport)-[n:MONTHLY_FLIGHTS]->(b:Airport)
	WHERE n.frequency=max_month_freq
	RETURN year, month, a.code AS origin, a.city AS origin_city, a.state AS origin_state, b.code AS dest, b.city AS dest_city, b.state AS dest_state, n.frequency AS frequency
	""")

c = datetime.datetime.now().replace(microsecond=0)
print "Time to read: " + str(c-b) + "\n"

# Query 1.b
print "================================================="
print "Query #1.b: Find the most frequent route per year"
print "-------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)
graph.cypher.execute("""
	MATCH (a:Airport)-[d:MONTHLY_FLIGHTS]->(b:Airport)
	WITH a, b, d.year AS year, sum(toInt(d.frequency)) AS yearly_freq
	CREATE (a)-[m:YEARLY_FLIGHT { year : year, frequency : yearly_freq }]->(b)
	"""
)

b = datetime.datetime.now().replace(microsecond=0)
print "Time to aggregate in yearly frequency: " + str(b-a)
print "-------------------------------------------------"

print graph.cypher.execute("""
	MATCH ()-[n:YEARLY_FLIGHTS]->()
	WITH n.year AS year, max(n.frequency) AS max_year_freq
	MATCH (a:Airport)-[n:YEARLY_FLIGHTS]->(b:Airport)
	WHERE n.frequency=max_year_freq
	RETURN year, a.code AS origin, a.city AS origin_city, a.state AS origin_state, b.code AS dest, b.city AS dest_city, b.state AS dest_state, n.frequency AS frequency
	""")

c = datetime.datetime.now().replace(microsecond=0)
print "Time to read: " + str(c-b) + "\n"

# Query 2
print "==================================================================="
print "Query #2: Find the airport with more flights (in and out) per month"
print "-------------------------------------------------------------------"

a = datetime.datetime.now().replace(microsecond=0)

print graph.cypher.execute("""
	MATCH (a:Airport)-[n:DAILY_FLIGHTS]-()
	WITH a.code AS code, n.year AS year, n.month AS month, sum(toInt(n.frequency)) as flights
	ORDER BY flights DESC
	RETURN year, month, collect(code)[0] AS airport, max(flights) AS tot_flights
	""")

b = datetime.datetime.now().replace(microsecond=0)
print "Time: " + str(b-a) + "\n"


# Query 2.b
print "===================================================================="
print "Query #2.b: Find the airport with more flights (in and out) per year"
print "--------------------------------------------------------------------"

a = datetime.datetime.now().replace(microsecond=0)

print graph.cypher.execute("""
	MATCH (a:Airport)-[n:DAILY_FLIGHTS]-()
	WITH a.code AS code, n.year AS year, sum(toInt(n.frequency)) as flights
	ORDER BY flights DESC
	RETURN year, collect(code)[0] AS airport, max(flights) AS tot_flights
	""")

b = datetime.datetime.now().replace(microsecond=0)
print "Time: " + str(b-a) + "\n"

