import os
import csv
import uuid
import boto3
from faker import Faker
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv


load_dotenv()

MONGO_HOST = os.getenv("MONGO_HOST", "172.31.19.141")  
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

fake = Faker()

# Conectar a MongoDB
mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
db = mongo_client["ingesta01Mongo"]
collection = db["feedback"]


def generate_feedback(n=20000):
    feedback_data = []
    for _ in range(n):
        feedback = {
            "_id": str(uuid.uuid4()),
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
    df.to_csv(filename, index=False)
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
    data = generate_feedback()
    insert_into_mongo(data)
    csv_file = save_to_csv(data)
    upload_to_s3(csv_file)