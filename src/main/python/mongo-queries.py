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
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "origin": { "$first": "$origin.code" },
        "destination": { "$first": "$destination.code" }
      }
    },
    { "$sort": { "monthly_freq": -1 } },
    {"$group": {
        "_id": {"year":"$year", "month":"$month"},
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "monthly_freq": {"$max": "$monthly_freq"},
        "origin": { "$first": "$origin" },
        "destination": { "$first": "$destination" }
      }
    },
    { "$project": {"_id": 0, "year": 1, "month": 1 , "monthly_freq": 1, "origin": 1, "destination": 1 } }
  ],
  allowDiskUse=True
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime:" + str(b-a)


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
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "origin": { "$first": "$origin" },
        "destination": { "$first": "$destination" }
      }
    },
    { "$sort": { "monthly_freq": -1 } },
    {"$group": {
        "_id": {"year":"$year", "month":"$month"},
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "monthly_freq": {"$max": "$monthly_freq"},
        "origin": { "$first": "$origin" },
        "destination": { "$first": "$destination" }
      }
    },
    { "$project": {"_id": 0, "year": 1, "month": 1 , "monthly_freq": 1, "origin": 1, "destination": 1 } }
  ],
  allowDiskUse=True
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime: " + str(b-a)

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
        "year": { "$first": "$year" },
        "origin": { "$first": "$origin.code" },
        "destination": { "$first": "$destination.code" }
      }
    },
    { "$sort": { "yearly_freq": -1 } },
    { "$group": {
        "_id": {"year":"$year"},
        "year": { "$first": "$year" },
        "yearly_freq": {"$max": "$yearly_freq"},
        "origin": { "$first": "$origin" },
        "destination": { "$first": "$destination" }
      }
    },
    { "$project": {"_id": 0, "year": 1, "yearly_freq": 1, "origin": 1, "destination": 1 } }
  ],
  allowDiskUse=True
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime: " + str(b-a)

# Query 3
print "======================================================================="
print "Query #3: Find the airport with more flights (in and out) per month"
print "-----------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)

cursor = db.routes.aggregate(
  [
    {
      "$group": {
        "_id": {"year":"$year", "month":"$month", "origin":"$origin.code","destination":"$destination.code"},
        "monthly_freq": {"$sum": "$frequency"},
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "origin": { "$first": "$origin.code" },
        "destination": { "$first": "$destination.code" }
      }
    },
    { 
      "$project": {
        "_id": 0, 
        "airportFrequencies":
        [
          { 
            "year": "$year",
            "month": "$month",
            "airport": "$origin",
            "monthly_freq": "$monthly_freq"
          },
          { 
            "year": "$year",
            "month": "$month",
            "airport": "$destination",
            "monthly_freq": "$monthly_freq"
          }
        ]
      } 
    },
    { "$unwind": "$airportFrequencies" },
    {
      "$project":{
        "year": "$airportFrequencies.year",
        "month": "$airportFrequencies.month",
        "airport": "$airportFrequencies.airport",
        "monthly_freq": "$airportFrequencies.monthly_freq"
      }
    },
    {
      "$group":{
        "_id": { "year":"$year", "month":"$month", "airport":"$airport" },
        "monthly_freq_in_out": {"$sum": "$monthly_freq"},
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "airport": { "$first": "$airport" }
      }
    },
    { "$sort": { "monthly_freq_in_out": -1 } },
    {
      "$group": {
        "_id": {"year": "$year", "month": "$month"},
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "monthly_freq_in_out": {"$max": "$monthly_freq_in_out"},
        "airport": { "$first": "$airport" }
      }
    },
    { "$project": {"_id":0, "year": 1, "month": 1, "airport": 1, "monthly_freq_in_out": 1}}
  ],
  allowDiskUse=True
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime: " + str(b-a)


# Query 4
print "======================================================================="
print "Query #4: Find the airport with more flights (in and out) per year"
print "-----------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)

cursor = db.routes.aggregate(
  [
    {
      "$group": {
        "_id": {"year":"$year", "origin":"$origin.code","destination":"$destination.code"},
        "yearly_freq": {"$sum": "$frequency"},
        "year": { "$first": "$year" },
        "origin": { "$first": "$origin.code" },
        "destination": { "$first": "$destination.code" }
      }
    },
    { 
      "$project": {
        "_id": 0, 
        "airportFrequencies": [
          { 
            "year": "$year",
            "airport": "$origin",
            "yearly_freq": "$yearly_freq"
          },
          { 
            "year": "$year",
            "airport": "$destination",
            "yearly_freq": "$yearly_freq"
          }
        ]
      } 
    },
    { "$unwind": "$airportFrequencies" },
    {
      "$project":{
        "year": "$airportFrequencies.year",
        "airport": "$airportFrequencies.airport",
        "yearly_freq": "$airportFrequencies.yearly_freq"
      }
    },
    {
      "$group":{
        "_id": { "year":"$year", "airport":"$airport" },
        "yearly_freq_in_out": {"$sum": "$yearly_freq"},
        "year": { "$first": "$year" },
        "airport": { "$first": "$airport" }
      }
    },
    { "$sort": { "yearly_freq_in_out": -1 } },
    {
      "$group": {
        "_id": {"year": "$year"},
        "year": { "$first": "$year" },
        "yearly_freq_in_out": {"$max": "$yearly_freq_in_out"},
        "airport": { "$first": "$airport" }
      }
    },
    { "$project": {"_id":0, "year": 1, "airport": 1, "yearly_freq_in_out": 1}}
  ],
  allowDiskUse=True
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime: " + str(b-a)

# Query 6
print "======================================================================="
print "Query #6: Find the state with more internal flights (per month)"
print "-----------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)

cursor = db.routes.aggregate(
  [ 
    {
      "$project": {
        "year": "$year",
        "month": "$month",
        "state": "$origin.state",
        "frequency": "$frequency",
        "same_state": { "$strcasecmp": ["$origin.state","$destination.state"] }
      }
    },
    { "$match": { "same_state": { "$eq": 0 } } },
    {
      "$group": {
        "_id": {"year":"$year", "month":"$month", "state":"$state"},
        "monthly_freq": {"$sum": "$frequency"},
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "state": { "$first": "$state" }
      }
    },
    { "$sort": { "monthly_freq": -1 } },
    {
      "$group": {
        "_id": {"year": "$year", "month":"$month"},
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "state": { "$first": "$state" },
        "monthly_freq": {"$max": "$monthly_freq"}
      }
    },
    { "$project": {"_id":0, "year": 1, "month": 1, "monthly_freq": 1, "state": 1}}
  ],
  allowDiskUse=True
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime: " + str(b-a)

# Query 6.b
print "======================================================================="
print "Query #6.b: Find the state with more internal flights (per year)"
print "-----------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)

cursor = db.routes.aggregate(
  [ 
    {
      "$project": {
        "year": "$year",
        "state": "$origin.state",
        "frequency": "$frequency",
        "same_state": { "$strcasecmp": ["$origin.state","$destination.state"] }
      }
    },
    { "$match": { "same_state": { "$eq": 0 } } },
    {
      "$group": {
        "_id": {"year":"$year", "state":"$state"},
        "yearly_freq": {"$sum": "$frequency"},
        "year": { "$first": "$year" },
        "state": { "$first": "$state" }
      }
    },
    { "$sort": { "yearly_freq": -1 } },
    {
      "$group": {
        "_id": {"year": "$year"},
        "year": { "$first": "$year" },
        "state": { "$first": "$state" },
        "yearly_freq": {"$max": "$yearly_freq"}
      }
    },
    { "$project": {"_id":0, "year": 1, "yearly_freq": 1, "state": 1}}
  ],
  allowDiskUse=True
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime: " + str(b-a)

# Query 7
print "======================================================================="
print "Query #7: Find the state with more departure flights to another state (per month)"
print "-----------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)

cursor = db.routes.aggregate(
  [
    {
      "$project": {
        "year": "$year",
        "month": "$month",
        "state": "$origin.state",
        "frequency": "$frequency",
        "same_state": { "$strcasecmp": ["$origin.state","$destination.state"] }
      }
    },
    { "$match": { "same_state": { "$ne": 0 } } },
    {
      "$group": {
        "_id": {"year":"$year", "month":"$month", "state":"$state"},
        "monthly_freq": {"$sum": "$frequency"},
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "state": { "$first": "$state" }
      }
    },
    { "$sort": { "monthly_freq": -1 } },
    {
      "$group": {
        "_id": {"year": "$year", "month":"$month"},
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "state": { "$first": "$state" },
        "monthly_freq": {"$max": "$monthly_freq"}
      }
    },
    { "$project": {"_id":0, "year": 1, "month": 1, "monthly_freq": 1, "state": 1}}
  ],
  allowDiskUse=True
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime: " + str(b-a)

# Query 7.b
print "======================================================================="
print "Query #7.b: Find the state with more departure flights to another state (per year)"
print "-----------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)

cursor = db.routes.aggregate(
  [
    {
      "$project": {
        "year": "$year",
        "state": "$origin.state",
        "frequency": "$frequency",
        "same_state": { "$strcasecmp": ["$origin.state","$destination.state"] }
      }
    },
    { "$match": { "same_state": { "$ne": 0 } } },
    {
      "$group": {
        "_id": {"year":"$year", "state":"$state"},
        "yearly_freq": {"$sum": "$frequency"},
        "year": { "$first": "$year" },
        "state": { "$first": "$state" }
      }
    },
    { "$sort": { "yearly_freq": -1 } },
    {
      "$group": {
        "_id": {"year": "$year"},
        "year": { "$first": "$year" },
        "state": { "$first": "$state" },
        "yearly_freq": {"$max": "$yearly_freq"}
      }
    },
    { "$project": {"_id":0, "year": 1, "yearly_freq": 1, "state": 1}}
  ],
  allowDiskUse=True
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime: " + str(b-a)

# Query 8
print "======================================================================="
print "Query #8: Find the state with more arrival flights from another state (per month)"
print "-----------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)

cursor = db.routes.aggregate(
  [
    {
      "$project": {
        "year": "$year",
        "month": "$month",
        "state": "$destination.state",
        "frequency": "$frequency",
        "same_state": { "$strcasecmp": ["$origin.state","$destination.state"] }
      }
    },
    { "$match": { "same_state": { "$ne": 0 } } },
    {
      "$group": {
        "_id": {"year":"$year", "month":"$month", "state":"$state"},
        "monthly_freq": {"$sum": "$frequency"},
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "state": { "$first": "$state" }
      }
    },
    { "$sort": { "monthly_freq": -1 } },
    {
      "$group": {
        "_id": {"year": "$year", "month":"$month"},
        "year": { "$first": "$year" },
        "month": { "$first": "$month" },
        "state": { "$first": "$state" },
        "monthly_freq": {"$max": "$monthly_freq"}
      }
    },
    { "$project": {"_id":0, "year": 1, "month": 1, "monthly_freq": 1, "state": 1}}
  ],
  allowDiskUse=True
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime: " + str(b-a)

# Query 8.b
print "======================================================================="
print "Query #8.b: Find the state with more arrival flights from another state (per year)"
print "-----------------------------------------------------------------------"
a = datetime.datetime.now().replace(microsecond=0)

cursor = db.routes.aggregate(
  [
    {
      "$project": {
        "year": "$year",
        "state": "$destination.state",
        "frequency": "$frequency",
        "same_state": { "$strcasecmp": ["$origin.state","$destination.state"] }
      }
    },
    { "$match": { "same_state": { "$ne": 0 } } },
    {
      "$group": {
        "_id": {"year":"$year", "state":"$state"},
        "yearly_freq": {"$sum": "$frequency"},
        "year": { "$first": "$year" },
        "state": { "$first": "$state" }
      }
    },
    { "$sort": { "yearly_freq": -1 } },
    {
      "$group": {
        "_id": {"year": "$year"},
        "year": { "$first": "$year" },
        "state": { "$first": "$state" },
        "yearly_freq": {"$max": "$yearly_freq"}
      }
    },
    { "$project": {"_id":0, "year": 1, "yearly_freq": 1, "state": 1}}
  ],
  allowDiskUse=True
)

b = datetime.datetime.now().replace(microsecond=0)

for document in cursor:
  print(document)

print "\nTime: " + str(b-a)