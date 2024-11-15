import psycopg2
import geopandas as gpd
from shapely.ops import unary_union
import config_stock

# Connexion à la base de données Analyse Stock Alluvial 
def connect_bdd():
    try:
        conn = psycopg2.connect(
            dbname=config_stock.DB_NAME,
            user=config_stock.DB_USER,
            password=config_stock.DB_PASSWORD,
            host=config_stock.DB_HOST,
            port=config_stock.DB_PORT
    )

        print("Connexion à la base de données réussie")
    except Exception as e:
        print("Echec de la connexion à la Base de Données")
        print(e)
        exit(1)
        
        conn.close()
    
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
        gdf_table1 = gpd.read_postgis(query_table1, conn, geom_col='geom')
        gdf_table2 = gpd.read_postgis(query_table2, conn, geom_col='geom')

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
    except Exception as e:
        exit(1)

    merged_gdf = gdf_table1._append(gdf_table2, ignore_index=True)
    dissolved_gdf = merged_gdf.dissolve()
    print("Fusion et regroupement réussis")
    
    dissolved_gdf['geom'] = dissolved_gdf['geom'].apply(lambda geom: geom.buffer(0))
    print ("Ton polygone est tout propre ! ")   

    exit(1)
    return dissolved_gdf

gdf_table1, gdf_table2 = connect_bdd()
result = traitement_spatiaux(gdf_table1, gdf_table2)

