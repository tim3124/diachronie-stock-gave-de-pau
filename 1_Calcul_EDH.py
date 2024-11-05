import psycopg2
import geopandas as gpd
from shapely.ops import unary_union
import logging


# Connexion à la base de données PostgreSQL
try:
    conn = psycopg2.connect(
        dbname="Analyse_Stock_Alluvial",
        user="timmadoulaud",
        password="Dordogne24@",
        host="localhost",
        port="5432"
)

    print("Connexion à la base de données réussie")
except Exception as e:
    print("Echec de la connexion à la Base de Données")
    print(e)
    exit(1)

