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
        "origin" : { "$first" : "$origin.code" },
        "destination" : { "$first" : "$destination.code" }
      }
    }
  ]
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime to aggregate in monthly frequency: " + str(b-a)
print "-----------------------------------------------------------------------"