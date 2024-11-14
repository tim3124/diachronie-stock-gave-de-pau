import psycopg2
import geopandas as gpd
from shapely.ops import unary_union
import logging

# Connexion à la base de données Analyse Stock Alluvial 
def connect_bdd():
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
    
    try: 
    # Requête pour récupérer les données des tables 'BA_2021' et 'BA_2018' dans le schéma 'ba'
        query_table1 = "SELECT * FROM ba.bande_active_fluviale_2021_gdp"
        query_table2 = "SELECT * FROM ba.bande_active_fluviale_2018_gdp_vf"

        print ("Requête réussie")
    except Exception as e2 : 
        print ("Voici une requête qui renvoie une erreur !")
        print (e2)
        exit(1)
    try : 
        gdf_table1 = gpd.GeoDataFrame.from_postgis(query_table1, conn, geom_col='geom')
        gdf_table2 = gpd.GeoDataFrame.from_postgis(query_table2, conn, geom_col='geom')

        print ("Géométrie récuperée !!")
    except Exception as e3 : 
        print ("Géométrie non récuperée :(")
        print (e3)
        exit(1)
    return (gdf_table1, gdf_table2)

def traitement_spatiaux(gdf_table1, gdf_table2):
    try:
        merged_gdf = gdf_table1._append(gdf_table2, ignore_index=True)
        dissolved_gdf = merged_gdf.dissolve()
        logging.info("Fusion et regroupement réussis")
    except Exception as e:
        logging.error("Erreur lors de la fusion et du regroupement")
        logging.error(e)
    exit(1)
    return dissolved_gdf

gdf_table1, gdf_table2 = connect_bdd()
result = traitement_spatiaux(gdf_table1, gdf_table2)

