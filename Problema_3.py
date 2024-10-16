import os
import requests
import zipfile
import pandas as pd
from pymongo import MongoClient
############################ Descargar / Descomprimir #################################
url = "https://netsg.cs.sfu.ca/youtubedata/0302.zip"
zip_file_path = "youtube_data.zip"

response = requests.get(url)
with open(zip_file_path, "wb") as f:
    f.write(response.content)

print(f"Se descargo el Archivo: {zip_file_path}")

extraer_carpeta = "youtube_data"
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extraer_carpeta)

print(f"Se descomprimio el archivo en: {extraer_carpeta}")

#################### Listo y leo los archivos ###########################

archivos_0302_en_carpeta = os.listdir(os.path.join(extraer_carpeta, "0302"))
print("Archivos disponibles en la carpeta youtube_data/0302 :", archivos_0302_en_carpeta)

datos = os.path.join(extraer_carpeta, "0302", "0.txt")

try:
    df = pd.read_csv(datos, sep='\t', header=None, on_bad_lines='skip')
    print(df.head())  
except Exception as e:
    print(f"Error al leer el archivo: {e}")

#################### Nombres de colummnas ########################
df.columns = ['videoID', 'uploader', 'age', 'category', 'length', 'views', 'rate', 'ratings', 'comments', 'relatedIDs1',
              'relatedIDs2', 'relatedIDs3', 'relatedIDs4', 'relatedIDs5', 'relatedIDs6', 'relatedIDs7', 'relatedIDs8']

print(df.head())

########################## Filtrado #############################
df_filtered = df[['videoID', 'age', 'category', 'views', 'rate']]

categories_to_keep = ['Music', 'Film & Animation']
df_filtered = df_filtered[df_filtered['category'].isin(categories_to_keep)]

print("Datos filtrados por categorías:")
print(df_filtered.head())

################## Procesamiento de datos en Mongo Db #############################3
cadena_conexion_mongo = "mongodb+srv://kiaraalvamarinos:MFmiKqEHcNYcowC4@clustermongodb.7yxgk.mongodb.net/?retryWrites=true&w=majority&appName=ClusterMongoDB"
try:
    
    client = MongoClient(cadena_conexion_mongo)
    db = client['youtube_db']
    collection = db['videos_data']

  
    data_to_insert = df_filtered.to_dict(orient='records')

    
    result = collection.insert_many(data_to_insert)
    print(f"Datos insertados en MongoDB")  

except Exception as e:
    print(f"Error al insertar datos en MongoDB: {e}")
