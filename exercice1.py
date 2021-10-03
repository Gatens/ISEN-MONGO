#-----------------------------
# Cloud Computing
# Projet 1 Ex 1
# SOETENS Gatien BRUFAU Thomas
#-----------------------------

# Import des modules
import pymongo
from pymongo import MongoClient
import requests
import json
import config

# Connexion au cluster
cluster=MongoClient("mongodb+srv://"+config.username+config.pwd+config.url)


db=cluster["bycicle_services"]


# Test
"""
collection=db['test']
post1={"_id":0,"name":"Gat","value":"test1"}
document=collection.insert_one(post1)
"""
# LILLE
def get_vLille():
    url="https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=%26&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
    response=requests.request('GET',url)
    response_json=json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

collection=db["Lille"]
collection.insert_many(get_vLille())
# RENNES
def get_vRennes():
    url = "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=stations_vls&q=%26&rows=3000"
    response = requests.request("GET",url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

collection=db["Rennes"]
collection.insert_many(get_vRennes())

# LYON
def get_vLyon():
    url="https://api.jcdecaux.com/vls/v3/stations?apiKey=frifk0jbxfefqqniqez09tw4jvk37wyf823b5j1i&contract=lyon"
    response=requests.request('GET',url)
    response_json=json.loads(response.text.encode('utf8'))
    return response_json
    #return response_json.get("records", [])

collection=db["Lyon"]
collection.insert_many(get_vLyon())

# PARIS
def get_vParis():
    url="https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=2000&facet=name&facet=is_installed&facet=is_renting&facet=is_returning&facet=nom_arrondissement_communes&refine.nom_arrondissement_communes=Paris"
    response=requests.request('GET',url)
    response_json=json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

collection=db["Lyon"]
collection.insert_many(get_vParis())
