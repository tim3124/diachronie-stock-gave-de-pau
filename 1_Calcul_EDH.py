import psycopg2
import geopandas as gpd
from shapely.ops import unary_union
import config_stock

# Connexion à la base de données Analyse Stock Alluvial 
def connect_bdd():
        conn = psycopg2.connect(
            dbname=config_stock.DB_NAME,
            user=config_stock.DB_USER,
            password=config_stock.DB_PASSWORD,
            host=config_stock.DB_HOST,
            port=config_stock.DB_PORT
    )

        print("Connexion à la base de données...")
        conn.close()
    
    # Requête pour récupérer les données des tables 'BA_2021' et 'BA_2018' dans le schéma 'ba'
        query_table1 = "SELECT * FROM ba.bande_active_fluviale_2021_gdp"
        query_table2 = "SELECT * FROM ba.bande_active_fluviale_2018_gdp_vf"

        print ("Requête réalisée")
        gdf_table1 = gpd.read_postgis(query_table1, conn, geom_col='geom')
        gdf_table2 = gpd.read_postgis(query_table2, conn, geom_col='geom')

        print ("Géométrie récuperée !!")
        print ("Géométrie non récuperée :(")
        return (gdf_table1, gdf_table2)

def traitement_spatiaux(gdf_table1, gdf_table2):
    merged_gdf = gdf_table1._append(gdf_table2, ignore_index=True)
    dissolved_gdf = merged_gdf.dissolve()
    
    #Fusion et regroupement des bandes actives
    merged_gdf = gdf_table1._append(gdf_table2, ignore_index=True)
    dissolved_gdf = merged_gdf.dissolve()
    print("Fusion et regroupement réussis")
    
    dissolved_gdf['geom'] = dissolved_gdf['geom'].apply(lambda geom: geom.buffer(0))
    print ("Ton polygone est tout propre ! ")
    
    #Création d'une zone tampon
    buffered_gdf = dissolved_gdf.buffer(100)
    print ("Tampon réalisé !")
    
    #Simplification des contours de l'EDH
    simplified_gdf = buffered_gdf.simplify(tolerance=50, preserve_topology=True)
    print ("Correction des contours de la bande active ! ") 
    
    #Conversion en geodataframe puis exportation en shapefile
    
    simplified_gdf = gpd.GeoDataFrame(geometry=simplified_gdf, crs=gdf_table1.crs)
    output_file = "etape_1_TEST.shp"
    simplified_gdf.to_file(output_file, driver='ESRI Shapefile')
    print ("Création de l'Enveloppe de Divagation Historique crée ! ")

    return dissolved_gdf, buffered_gdf, simplified_gdf, output_file

gdf_table1, gdf_table2 = connect_bdd()
result = traitement_spatiaux(gdf_table1, gdf_table2)

