
#-----------------------------
# Cloud Computing
# Projet 1 Ex 2
# SOETENS Gatien BRUFAU Thomas
#-----------------------------

# Import des modules
import pymongo
from pymongo import MongoClient
import requests
import json
import time
import config

# Connexion au cluster
cluster=MongoClient("mongodb+srv://"+config.username+config.pwd+config.url)
db=cluster["bycicle_services"]
collection = db["Lille_historique"]



def get_vlille():
    url = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=%26&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
    response = requests.request("GET",url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

def refresh(duree):
    collection.insert_many(get_vlille()) # on rerempli la base de données avec les nouvelles infos
    time.sleep(duree)
    #collection.remove() # on "nettoie" la collection correspondante
    print("refresh")

while True:
    refresh(60) # on rafraichit la database toutes les 60 secondes

    