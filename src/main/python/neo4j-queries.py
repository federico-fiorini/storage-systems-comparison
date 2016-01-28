import datetime
from py2neo import Graph, Node, Relationship

graph = Graph()

# Query 1
print "======================================================================="
print "Query #1: Find the most frequent route per month (with only airport ID)"
print "-----------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)
graph.cypher.execute("""
	MATCH (a:Airport)-[d:DAILY_FLIGHT]->(b:Airport)
	WITH a, b, d.year AS year, d.month AS month, sum(toInt(d.frequency)) AS monthly_freq
	CREATE (a)-[m:MONTHLY_FLIGHT { year : year, month : month, frequency : monthly_freq }]->(b)
	"""
)

b = datetime.datetime.now().replace(microsecond=0)
print "Time to aggregate in monthly frequency: " + str(b-a)
print "-----------------------------------------------------------------------"

print graph.cypher.execute("""
	MATCH ()-[n:MONTHLY_FLIGHT]->()
	WITH n.year AS year, n.month AS month, max(n.frequency) AS max_month_freq
	MATCH (a:Airport)-[n:MONTHLY_FLIGHT]->(b:Airport)
	WHERE n.frequency=max_month_freq
	RETURN year, month, a.id AS origin_id, b.id AS dest_id, n.frequency AS frequency
	""")

c = datetime.datetime.now().replace(microsecond=0)
print "Time to read: " + str(c-b) + "\n"


# Query 1.b
print "=========================================================================================="
print "Query #1.b: Find the most frequent route per month with airports information (city, state)"
print "------------------------------------------------------------------------------------------"

a = datetime.datetime.now().replace(microsecond=0)

print graph.cypher.execute("""
	MATCH ()-[n:MONTHLY_FLIGHT]->()
	WITH n.year AS year, n.month AS month, max(n.frequency) AS max_month_freq
	MATCH (a:Airport)-[n:MONTHLY_FLIGHT]->(b:Airport)
	WHERE n.frequency=max_month_freq
	RETURN year, month, a.id AS origin_id, a.city AS origin_city, a.state AS origin_state, b.id AS dest_id, b.city AS dest_city, b.state AS dest_state, n.frequency AS frequency
	""")


b = datetime.datetime.now().replace(microsecond=0)
print "Time to read: " + str(b-a) + "\n"

# Query 2
print "======================================================================="
print "Query #2: Find the most frequent route per year (with only airport ID)"
print "-----------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)
graph.cypher.execute("""
	MATCH (a:Airport)-[d:MONTHLY_FLIGHT]->(b:Airport)
	WITH a, b, d.year AS year, sum(toInt(d.frequency)) AS yearly_freq
	CREATE (a)-[m:YEARLY_FLIGHT { year : year, frequency : yearly_freq }]->(b)
	"""
)

b = datetime.datetime.now().replace(microsecond=0)
print "Time to aggregate in yearly frequency: " + str(b-a)
print "-----------------------------------------------------------------------"

print graph.cypher.execute("""
	MATCH ()-[n:YEARLY_FLIGHT]->()
	WITH n.year AS year, max(n.frequency) AS max_year_freq
	MATCH (a:Airport)-[n:YEARLY_FLIGHT]->(b:Airport)
	WHERE n.frequency=max_year_freq
	RETURN year, a.id AS origin_id, b.id AS dest_id, n.frequency AS frequency
	""")

c = datetime.datetime.now().replace(microsecond=0)
print "Time to read: " + str(c-b) + "\n"


# Query 2.b
print "=========================================================================================="
print "Query #2.b: Find the most frequent route per year with airports information (city, state)"
print "------------------------------------------------------------------------------------------"

a = datetime.datetime.now().replace(microsecond=0)

print graph.cypher.execute("""
	MATCH ()-[n:YEARLY_FLIGHT]->()
	WITH n.year AS year, max(n.frequency) AS max_year_freq
	MATCH (a:Airport)-[n:YEARLY_FLIGHT]->(b:Airport)
	WHERE n.frequency=max_year_freq
	RETURN year, a.id AS origin_id, a.city AS origin_city, a.state AS origin_state, b.id AS dest_id, b.city AS dest_city, b.state AS dest_state, n.frequency AS frequency
	""")


b = datetime.datetime.now().replace(microsecond=0)
print "Time to read: " + str(b-a) + "\n"
