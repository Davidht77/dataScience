import os
import csv
import uuid
import boto3
import psycopg2 # Nueva importación para PostgreSQL
import random # Para seleccionar clientes aleatoriamente
from faker import Faker
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv


load_dotenv()

MONGO_HOST = os.getenv("MONGO_HOST", "172.31.19.141")  
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

# Variables de entorno para PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

fake = Faker()

# Conectar a MongoDB
if MONGO_USER and MONGO_PASSWORD:
    mongo_client = MongoClient(MONGO_HOST, MONGO_PORT, username=MONGO_USER, password=MONGO_PASSWORD)
else:
    mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
db = mongo_client["ingesta01Mongo"]
collection = db["feedback"]

# Conectar a PostgreSQL y obtener client IDs
def get_postgres_client_ids():
    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB
        )
        cur = conn.cursor()
        # Asumimos que tienes una tabla llamada 'clients' con una columna 'client_id'
        # Ajusta la consulta según tu esquema de base de datos
        cur.execute("SELECT id FROM cliente") 
        client_ids = [row[0] for row in cur.fetchall()]
        cur.close()
        return client_ids
    except (Exception, psycopg2.Error) as error:
        print(f"Error al conectar o consultar PostgreSQL: {error}")
        return [] # Retornar lista vacía en caso de error para no bloquear el script
    finally:
        if conn:
            conn.close()

def generate_feedback(n=20000, client_ids=None):
    if not client_ids:
        print("No se pudieron obtener client IDs de PostgreSQL, se generarán aleatoriamente.")
    feedback_data = []
    for _ in range(n):
        feedback = {
            "feedbackId": str(uuid.uuid4()),
            "clientId": random.choice(client_ids) if client_ids else str(uuid.uuid4()), # Usar ID de cliente de PostgreSQL o generar uno nuevo si no hay
            "comment": fake.sentence(nb_words=10),
            "rating": fake.random_int(min=1, max=5),
            "createdAt": fake.date_time_this_year().isoformat()
        }
        feedback_data.append(feedback)
    return feedback_data

# Insertar en MongoDB
def insert_into_mongo(data):
    collection.insert_many(data)
    print(f"{len(data)} documentos insertados en MongoDB.")

# Guardar como CSV
def save_to_csv(data, filename="feedback.csv"):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, header=False)
    print(f"Archivo CSV generado: {filename}")
    return filename

# Subir a S3
def upload_to_s3(file_name):
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN
        )
        s3_client.upload_file(file_name, BUCKET_NAME, file_name)
        print(f"Archivo {file_name} subido a S3 correctamente.")
    except Exception as e:
        print(f"Error al subir archivo a S3: {e}")


if __name__ == "__main__":
    # Obtener client IDs de PostgreSQL
    postgres_client_ids = get_postgres_client_ids()

    data = generate_feedback(client_ids=postgres_client_ids)
    if data: # Solo proceder si se generaron datos
        insert_into_mongo(data)
    csv_file = save_to_csv(data)
    upload_to_s3(csv_file)