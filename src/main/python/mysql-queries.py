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


def runQueryAndGetTime(sql, n=10):
  totalTime = datetime.timedelta(0)
  results = None

  for i in range(n):
    a = datetime.datetime.now()
    query.execute(sql)
    results = query.fetchall()
    b = datetime.datetime.now()
    totalTime += b-a

  return (results, totalTime/n)

# Query 1
print "================================================"
print "Query #1: Find the most frequent route per month"

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
(results, time) = runQueryAndGetTime(sql)

table = PrettyTable(["year", "month", "origin", "origin_city", "origin_state", "destination", "destination_city", "destination_state", "max_monthly_freq"])
for row in results:
	table.add_row(row)
print table
print "Time: " + str(time) + "\n"

# Query 1.b
print "================================================="
print "Query #1.b: Find the most frequent route per year"

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
(results, time) = runQueryAndGetTime(sql)

table = PrettyTable(["year", "origin", "origin_city", "origin_state", "destination", "destination_city", "destination_state", "max_yearly_freq"])
for row in results:
	table.add_row(row)
print table
print "Time: " + str(time) + "\n"


# Query 2
print "======================================================================="
print "Query #2: Find the airport with more flights (in and out) per month"

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
(results, time) = runQueryAndGetTime(sql)

table = PrettyTable(["year", "month", "airport", "tot_flights"])
for row in results:
	table.add_row(row)
print table
print "Time: " + str(time) + "\n"

# Query 2.b
print "======================================================================="
print "Query #2.b: Find the airport with more flights (in and out) per year"

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
(results, time) = runQueryAndGetTime(sql)

table = PrettyTable(["year", "airport", "tot_flights"])
for row in results:
	table.add_row(row)
print table
print "Time: " + str(time) + "\n"
query.close()

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

a = datetime.datetime.now()
sql = """
	DROP TABLE IF EXISTS dijnodes,dijpaths; 
	CREATE TABLE dijnodes ( 
	  nodeID int PRIMARY KEY AUTO_INCREMENT NOT NULL, 
	  nodename varchar (20) NOT NULL, 
	  cost int NULL, 
	  pathID int NULL, 
	  calculated tinyint NOT NULL  
	); 

	CREATE TABLE dijpaths ( 
	  pathID int PRIMARY KEY AUTO_INCREMENT, 
	  fromNodeID int NOT NULL , 
	  toNodeID int NOT NULL , 
	  cost int NOT NULL 
	); 
	"""
query.execute(sql)
query.fetchall()
query.close()

sql = """
	DROP PROCEDURE IF EXISTS dijAddPath; 
	CREATE PROCEDURE dijAddPath(
		pFromNodeName VARCHAR(20), pToNodeName VARCHAR(20), pCost INT
	)
	BEGIN 
	  DECLARE vFromNodeID, vToNodeID, vPathID INT; 
	  SET vFromNodeID = ( SELECT NodeID FROM dijnodes WHERE NodeName = pFromNodeName ); 
	  IF vFromNodeID IS NULL THEN 
	    BEGIN 
	      INSERT INTO dijnodes (NodeName,Calculated) VALUES (pFromNodeName,0); 
	      SET vFromNodeID = LAST_INSERT_ID(); 
	    END; 
	  END IF; 
	  SET vToNodeID = ( SELECT NodeID FROM dijnodes WHERE NodeName = pToNodeName ); 
	  IF vToNodeID IS NULL THEN 
	    BEGIN 
	      INSERT INTO dijnodes(NodeName, Calculated)  
	      VALUES(pToNodeName,0); 
	      SET vToNodeID = LAST_INSERT_ID(); 
	    END; 
	  END IF; 
	  SET vPathID = ( SELECT PathID FROM dijpaths  
	                  WHERE FromNodeID = vFromNodeID AND ToNodeID = vToNodeID  
	                ); 
	  IF vPathID IS NULL THEN 
	    INSERT INTO dijpaths(FromNodeID,ToNodeID,Cost)  
	    VALUES(vFromNodeID,vToNodeID,pCost); 
	  ELSE 
	    UPDATE dijpaths SET Cost = pCost   
	    WHERE FromNodeID = vFromNodeID AND ToNodeID = vToNodeID; 
	  END IF; 
	END;  
	"""
query = conn.cursor()
query.execute(sql)
query.fetchall()
query.close()

sql = """
	SELECT r.origin, r.destination
	FROM flights.routes r
	WHERE r.year = "%s" AND r.month = "%s" AND r.day = "%s";
	""" % (year, month, day)

query = conn.cursor()
query.execute(sql)
for route in query.fetchall():
	sql = "call dijaddpath('%s', '%s',  1);" % (route[0], route[1])
	query.execute(sql)

query.close()

sql = """
	DROP PROCEDURE IF EXISTS dijResolve;
	CREATE PROCEDURE dijResolve( pFromNodeName VARCHAR(20), pToNodeName VARCHAR(20) )
	BEGIN
	  DECLARE vFromNodeID, vToNodeID, vNodeID, vCost, vPathID INT; 
	  DECLARE vFromNodeName, vToNodeName VARCHAR(20); 
	  -- null out path info in the nodes table 
	  UPDATE dijnodes SET PathID = NULL,Cost = NULL,Calculated = 0; 
	  -- find nodeIDs referenced by input params 
	  SET vFromNodeID = ( SELECT NodeID FROM dijnodes WHERE NodeName = pFromNodeName ); 
	  IF vFromNodeID IS NULL THEN 
	    SELECT CONCAT('From node name ', pFromNodeName, ' not found.' );  
	  ELSE 
	    BEGIN 
	      -- start at src node 
	      SET vNodeID = vFromNodeID; 
	      SET vToNodeID = ( SELECT NodeID FROM dijnodes WHERE NodeName = pToNodeName ); 
	      IF vToNodeID IS NULL THEN 
	        SELECT CONCAT('From node name ', pToNodeName, ' not found.' ); 
	      ELSE 
	        BEGIN 
	          -- calculate path costs till all are done 
	          UPDATE dijnodes SET Cost=0 WHERE NodeID = vFromNodeID; 
	          WHILE vNodeID IS NOT NULL DO 
	            BEGIN 
	              UPDATE  
	                dijnodes AS src 
	                JOIN dijpaths AS paths ON paths.FromNodeID = src.NodeID 
	                JOIN dijnodes AS dest ON dest.NodeID = Paths.ToNodeID 
	              SET dest.Cost = CASE 
	                                WHEN dest.Cost IS NULL THEN src.Cost + Paths.Cost 
	                                WHEN src.Cost + Paths.Cost < dest.Cost THEN src.Cost + Paths.Cost 
	                                ELSE dest.Cost 
	                              END, 
	                  dest.PathID = Paths.PathID 
	              WHERE  
	                src.NodeID = vNodeID 
	                AND (dest.Cost IS NULL OR src.Cost + Paths.Cost < dest.Cost) 
	                AND dest.Calculated = 0; 
	        
	              UPDATE dijnodes SET Calculated = 1 WHERE NodeID = vNodeID; 

	              SET vNodeID = ( SELECT nodeID FROM dijnodes 
	                              WHERE Calculated = 0 AND Cost IS NOT NULL 
	                              ORDER BY Cost LIMIT 1 
	                            ); 
	            END; 
	          END WHILE; 
	        END; 
	      END IF; 
	    END; 
	  END IF; 
	  IF EXISTS( SELECT 1 FROM dijnodes WHERE NodeID = vToNodeID AND Cost IS NULL ) THEN 
	    -- problem,  cannot proceed 
	    SELECT CONCAT( 'Node ',vNodeID, ' missed.' ); 
	  ELSE 
	    BEGIN 
	      -- write itinerary to map table 
	      DROP TEMPORARY TABLE IF EXISTS map; 
	      CREATE TEMPORARY TABLE map ( 
	        RowID INT PRIMARY KEY AUTO_INCREMENT, 
	        FromNodeName VARCHAR(20), 
	        ToNodeName VARCHAR(20), 
	        Cost INT 
	      ) ENGINE=MEMORY; 
	      WHILE vFromNodeID <> vToNodeID DO 
	        BEGIN 
	          SELECT  
	            src.NodeName,dest.NodeName,dest.Cost,dest.PathID 
	            INTO vFromNodeName, vToNodeName, vCost, vPathID 
	          FROM  
	            dijnodes AS dest 
	            JOIN dijpaths AS Paths ON Paths.PathID = dest.PathID 
	            JOIN dijnodes AS src ON src.NodeID = Paths.FromNodeID 
	          WHERE dest.NodeID = vToNodeID; 
	           
	          INSERT INTO Map(FromNodeName,ToNodeName,Cost) VALUES(vFromNodeName,vToNodeName,vCost); 
	           
	          SET vToNodeID = (SELECT FromNodeID FROM dijPaths WHERE PathID = vPathID); 
	        END; 
	      END WHILE; 
	      SELECT FromNodeName,ToNodeName,Cost FROM Map ORDER BY RowID DESC; 
	      DROP TEMPORARY TABLE Map; 
	    END; 
	  END IF; 
	END; 
	"""
query = conn.cursor()
query.execute(sql)
query.close()

sql = "CALL dijResolve('%s','%s');" % (departure, arrival)
query = conn.cursor()
query.execute(sql)

table = PrettyTable(["departure", "arrival", "route_part"])
for row in query.fetchall():
	table.add_row(row)

query.close()

sql = """
	DROP TABLE IF EXISTS dijnodes,dijpaths; 
	DROP PROCEDURE IF EXISTS dijAddPath; 
	DROP PROCEDURE IF EXISTS dijResolve;
	"""
query = conn.cursor()
query.execute(sql)
query.close()
query = conn.cursor()

b = datetime.datetime.now()
print table
print "Time: " + str(b-a) + "\n"

# Query 4
print "==============================================================="
print "Query #4: Find the state with more internal flights (per month)"

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
(results, time) = runQueryAndGetTime(sql)

table = PrettyTable(["year", "month", "state", "monthly_freq"])
for row in results:
	table.add_row(row)
print table
print "Time: " + str(time) + "\n"


# Query 4.b
print "================================================================"
print "Query #4.b: Find the state with more internal flights (per year)"

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
(results, time) = runQueryAndGetTime(sql)

table = PrettyTable(["year", "state", "yearly_freq"])
for row in results:
	table.add_row(row)
print table
print "Time: " + str(time) + "\n"


# Query 5
print "================================================================================="
print "Query #5: Find the state with more departure flights to another state (per month)"

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
(results, time) = runQueryAndGetTime(sql)

table = PrettyTable(["year", "month", "state", "monthly_freq"])
for row in results:
	table.add_row(row)
print table
print "Time: " + str(time) + "\n"


# Query 5.b
print "==================================================================================="
print "Query #5.b: Find the state with more departure flights to another state  (per year)"

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
(results, time) = runQueryAndGetTime(sql)

table = PrettyTable(["year", "state", "yearly_freq"])
for row in results:
	table.add_row(row)
print table
print "Time: " + str(time) + "\n"


# Query 6
print "================================================================================="
print "Query #6: Find the state with more arrival flights from another state (per month)"

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
(results, time) = runQueryAndGetTime(sql)

table = PrettyTable(["year", "month", "state", "monthly_freq"])
for row in results:
	table.add_row(row)
print table
print "Time: " + str(time) + "\n"

# Query 6.b
print "=================================================================================="
print "Query #6.b: Find the state with more arrival flights from another state (per year)"

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
(results, time) = runQueryAndGetTime(sql)

table = PrettyTable(["year", "state", "yearly_freq"])
for row in results:
	table.add_row(row)
print table
print "Time: " + str(time) + "\n"

