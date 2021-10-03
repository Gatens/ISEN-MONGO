#-----------------------------
# Cloud Computing
# Projet 1 Ex 4
# SOETENS Gatien BRUFAU Thomas
#-----------------------------

# Import des modules
import pymongo
from pymongo import MongoClient
import config

# Connexion au cluster
cluster=MongoClient("mongodb+srv://"+config.username+config.pwd+config.url)
db=cluster["bycicle_services"]
collection=db["Lille"]

#recherche une station par un nom (quelques lettres)
def findStation(recherche):
    result = collection.find({"fields.nom":{"$regex": recherche, "$options":'i'}})
    for i in result:
        print(i)

#findStation("militaire")

# Update a station
def update(station):
    collection.update_one(
        {"fields.nom":station},
        {'$set': {'fields.etat':'HORS SERVICE'}} # passe l'etat de la station a hors service
    )

#update("N.D. DE LA TREILLE")
# Remove station and data
def remove(station):
    query={"fields.nom":station}
    collection.delete_one(query)

#remove("N.D. DE LA TREILLE")

def deactivate(): #met les stations dans le polygon : HORS SERVICE
    result = collection.find({"geometry": { 
    "$near" : {
        "$geometry": {
            "type": "Polygon",
            "coordinates":[ 3.0629778380918538 ,50.64105303104528]
            },
        "$maxDistance": 300,
        "$minDistance": 0
}
}})
    for i in result:
        update(i["fields"]["nom"])
        

deactivate()

def ratiobike():
    collection = db["Lille"] 
    result = collection.aggregate([{ 
   "$group":{ 
       "_id": { "name":"$fields.nom","hourofday": { "$hour": "$record_timestamp" },"totalplace":{"$sum":["$fields.nbvelosdispo","$fields.nbplacesdispo"]} },
       "bike":{"$avg":"$nbvelosdispo"},
   },
   },
   {"$project" : {"_id":1, "ratio": { "$cond": [ { "$eq": [ "$_id.totalplace", 0 ] }, "N/A", {"$divide":["$bike","$_id.totalplace"]}]}}},
   { "$match": { "ratio" : { "$lt": 0.2 }}},
   ])
    
    return result

for i in ratiobike():
    print(i)