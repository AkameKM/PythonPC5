import pandas as pd
import sqlite3
from pymongo import MongoClient


df = pd.read_csv('/workspaces/PythonPC5/Problema_2/winemag-data-130k-v2.csv')
#Explorando el Datafranme
print(f"Vista General: \n{df.head()}")
print(f"Información General: \n{df.info()}")
print(f"Descripción de las variables: {df.describe()}")

#Renombro mi colummna
df.rename(columns={
    'country': 'nombre',
    'points': 'puntuacion',
    'price': 'precio',
    'variety': 'variedad'
}, inplace=True)

#Creo las colummnas
continentes = pd.read_csv('/workspaces/PythonPC5/Problema_2/continentes.csv')
print(continentes.head()) 
print(continentes.columns)

continentes.rename(columns={'country': 'nombre'}, inplace=True)

df = pd.merge(df, continentes[['nombre', 'continente']], on='nombre', how='left')

df['precio_por_puntuacion'] = df.apply(lambda row: row['precio'] / row['puntuacion'] if row['puntuacion'] > 0 else None, axis=1)

def clasificar_precio(precio):
    if precio < 10:
        return 'Barato'
    elif 10 <= precio < 30:
        return 'Medio'
    else:
        return 'Caro'

df['categoria_precio'] = df['precio'].apply(clasificar_precio)

#Creo los Reportes
# Reporte 1:
reporte1 = df.groupby('continente').agg({'puntuacion': 'max'}).reset_index()
reporte1 = reporte1.rename(columns={'puntuacion': 'puntuacion_max'})

# Reporte 2: 
reporte2 = df.groupby('nombre').agg({'precio': 'mean', 'title': 'count'}).reset_index()
reporte2 = reporte2.rename(columns={'precio': 'precio_promedio', 'title': 'cantidad_reviews'})

# Reporte 3: 
reporte3 = df.groupby('categoria_precio').agg({'precio': 'mean', 'puntuacion': 'mean'}).reset_index()

# Reporte 4: 
reporte4 = df.sort_values(by='puntuacion', ascending=False)[['nombre', 'variedad', 'puntuacion', 'precio']].head(10)

#Exporto los reportes
# Reporte 1 a CSV
reporte1.to_csv('reporte1_puntuacion_max.csv', index=False)

# Reporte 2 a Excel
reporte2.to_excel('reporte2_precio_promedio.xlsx', index=False)

# Reporte 3 a SQLite
cont = sqlite3.connect('reportes.db')
reporte3.to_sql('reporte_categoria_precio', cont, if_exists='replace', index=False)
cont.close()

# Reporte 4 a MongoDB
cadena_conexion_mongo = "mongodb+srv://kiaraalvamarinos:MFmiKqEHcNYcowC4@clustermongodb.7yxgk.mongodb.net/?retryWrites=true&w=majority&appName=ClusterMongoDB"
client = MongoClient(cadena_conexion_mongo)
db = client['reportes_db']
db.reporte_vinos.insert_many(reporte4.to_dict('records'))

print("Reportes exportados exitosamente.")


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email import encoders

import os

smtp_server = 'smtp.gmail.com'  
smtp_port = 587 
sender_email = 'kiaraalvamarinos@gmail.com'
sender_password = open('token.txt').read().strip() 

receiver_email = 'kiara.alvam@gmail.com'
subject = 'RE: Reporte de Vinos'
body = 'Estimados, Adjunto el reporte de vinos mejor puntuados por continente para su revision\n Saludos.'

mensaje = MIMEMultipart()
mensaje['From'] = sender_email
mensaje['To'] = receiver_email
mensaje['Subject'] = body


with open('reporte1_puntuacion_max.csv', 'rb') as adjunto:
    parte = MIMEBase('application', 'octet-stream')
    parte.set_payload(adjunto.read())
    encoders.encode_base64(parte)
    parte.add_header('Content-Disposition', f'attachment; filename=reporte1_puntuacion_max.csv')
    mensaje.attach(parte)

# Enviar el correo
with smtplib.SMTP('smtp.gmail.com', 587) as servidor:  # Cambia esto por tu servidor SMTP
    servidor.starttls()
    servidor.login(sender_email, sender_password)
    servidor.send_message(mensaje)

print("Correo enviado exitosamente.")
