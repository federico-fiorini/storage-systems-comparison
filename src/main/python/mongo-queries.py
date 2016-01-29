import datetime
from pymongo import MongoClient

client = MongoClient()
db = client.flights

# Query 1
print "======================================================================="
print "Query #1: Find the most frequent route per month (with only airport ID)"
print "-----------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)

cursor = db.routes.aggregate(
  [
    {"$group": {
        "_id": {"year":"$year", "month":"$month", "origin":"$origin.code","destination":"$destination.code"},
        "monthly_freq": {"$sum": "$frequency"},
        "year" : { "$first" : "$year" },
        "month" : { "$first" : "$month" },
        "origin" : { "$first" : "$origin.code" },
        "destination" : { "$first" : "$destination.code" }
      }
    },
    { "$sort": { "monthly_freq": -1 } },
    {"$group": {
        "_id": {"year":"$year", "month":"$month"},
        "year" : { "$first" : "$year" },
        "month" : { "$first" : "$month" },
        "monthly_freq": {"$max": "$monthly_freq"},
        "origin" : { "$first" : "$origin" },
        "destination" : { "$first" : "$destination" }
      }
    },
    { "$project": {"_id": 0, "year" : 1, "month": 1 , "monthly_freq" : 1, "origin" : 1, "destination" : 1 } }
  ]
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime to aggregate in monthly frequency: \n" + str(b-a)

# Query 1.b
print "=========================================================================================="
print "Query #1.b: Find the most frequent route per month with airports information (city, state)"
print "------------------------------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)

cursor = db.routes.aggregate(
  [
    {"$group": {
        "_id": {"year":"$year", "month":"$month", "origin":"$origin.code","destination":"$destination.code"},
        "monthly_freq": {"$sum": "$frequency"},
        "year" : { "$first" : "$year" },
        "month" : { "$first" : "$month" },
        "origin" : { "$first" : "$origin" },
        "destination" : { "$first" : "$destination" }
      }
    },
    { "$sort": { "monthly_freq": -1 } },
    {"$group": {
        "_id": {"year":"$year", "month":"$month"},
        "year" : { "$first" : "$year" },
        "month" : { "$first" : "$month" },
        "monthly_freq": {"$max": "$monthly_freq"},
        "origin" : { "$first" : "$origin" },
        "destination" : { "$first" : "$destination" }
      }
    },
    { "$project": {"_id": 0, "year" : 1, "month": 1 , "monthly_freq" : 1, "origin" : 1, "destination" : 1 } }
  ]
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime to aggregate in monthly frequency: \n" + str(b-a)

# Query 2
print "======================================================================="
print "Query #2: Find the most frequent route per year (with only airport ID)"
print "-----------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)

cursor = db.routes.aggregate(
  [
    { "$group": {
        "_id": {"year":"$year", "origin":"$origin.code","destination":"$destination.code"},
        "yearly_freq": {"$sum": "$frequency"},
        "year" : { "$first" : "$year" },
        "origin" : { "$first" : "$origin.code" },
        "destination" : { "$first" : "$destination.code" }
      }
    },
    { "$sort": { "yearly_freq": -1 } },
    { "$group": {
        "_id": {"year":"$year"},
        "year" : { "$first" : "$year" },
        "yearly_freq": {"$max": "$yearly_freq"},
        "origin" : { "$first" : "$origin" },
        "destination" : { "$first" : "$destination" }
      }
    },
    { "$project": {"_id": 0, "year" : 1, "yearly_freq" : 1, "origin" : 1, "destination" : 1 } }
  ]
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime to aggregate in yearly frequency: \n" + str(b-a)