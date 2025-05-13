import uuid
import random
import boto3
import pandas as pd
import mysql.connector
import os
from faker import Faker
from dotenv import load_dotenv

load_dotenv() # Carga las variables de entorno desde .env

fake = Faker()


db_connection = mysql.connector.connect(
    host=os.environ.get('DB_HOST'),
    user=os.environ.get('DB_USER'),
    port=int(os.environ.get('DB_PORT')),
    password=os.environ.get('DB_PASSWORD'),
    database=os.environ.get('DB_NAME')
)
cursor = db_connection.cursor()

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.environ.get('AWS_SESSION_TOKEN')
)
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')


def generate_employees(sede_ids, n=20000):
    print("Generando empleados...")
    employees_data = []
    for _ in range(n):
        emp = {
            'id': str(uuid.uuid4()).replace('-', ''),
            'name': fake.first_name(),
            'last_name': fake.last_name(),
            'age': random.randint(18, 65),
            'phone': fake.phone_number()[:20],
            'email': fake.email(),
            'password': fake.password(),
            'imagenUrlKey': fake.image_url(),
            'salary': round(random.uniform(1500, 3500), 2),
            'role': random.choice(['Trainer', 'Nutricionist', 'Administrator']),
            'sede_id': random.choice(sede_ids)
        }
        employees_data.append(emp)

    insert_query = """
        INSERT INTO employees (id, name, lastName, age, phone, email, password, imagenUrlKey, salary, role, sedeId)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = [
        (e['id'], e['name'], e['last_name'], e['age'], e['phone'],
         e['email'], e['password'], e['imagenUrlKey'], e['salary'], e['role'], e['sede_id'])
        for e in employees_data
    ]
    cursor.executemany(insert_query, values)
    db_connection.commit()

    pd.DataFrame(employees_data).to_csv('employees.csv', index=False)
    upload_to_s3('employees.csv')
    print(f"{n} empleados insertados.")


# SUBIDA A S3
def upload_to_s3(file_name):
    try:
        s3_client.upload_file(file_name, BUCKET_NAME, file_name)
        print(f"Subido a S3: {file_name}")
    except Exception as e:
        print(f"Error al subir {file_name} a S3: {e}")



if __name__ == '__main__':
    cursor.execute("SELECT id FROM sede")
    sede_ids = [row[0] for row in cursor.fetchall()]
    print(f"{len(sede_ids)} sedes obtenidas de la base de datos.")
    generate_employees(sede_ids)
    cursor.close()
    db_connection.close()
    print("Ingesta finalizada.")