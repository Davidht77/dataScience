import os
import uuid
import random
import boto3
import pandas as pd
import psycopg2
from faker import Faker
from dotenv import load_dotenv


load_dotenv()


PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
BUCKET_NAME = os.getenv("BUCKET_NAME")


fake = Faker()


conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    user=PG_USER,
    password=PG_PASSWORD,
    database=PG_DATABASE
)
cursor = conn.cursor()


cursor.execute("SELECT id FROM plan")
plan_ids = [row[0] for row in cursor.fetchall()]
print(plan_ids)


def generate_clients(n=20000):
    print("Generando clientes...")
    clients = []
    for _ in range(n):
        client = {
            'id': str(uuid.uuid4()),
            'name': fake.first_name(),
            'lastname': fake.last_name(),
            'age': fake.random_int(min=18, max=80),
            'email': fake.unique.email(),
            'phone': fake.phone_number(),
            'password': fake.password(),
            'plan_id': random.choice(plan_ids)
        }
        clients.append(client)
    return clients

def insert_clients(clients):
    print("Insertando clientes...")
    insert_query = """
        INSERT INTO cliente (id, name, last_name, email, phone, age, password, id_plan)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    data = [(c['id'], c['name'], c['lastname'], c['email'], c['phone'], c['age'], c['password'], c['plan_id']) for c in clients]
    cursor.executemany(insert_query, data)
    conn.commit()


def generate_payments(clients):
    print("Generando pagos...")
    payments = []
    for client in clients:
        payment = {
            'id': str(uuid.uuid4()),
            'client_id': client['id'],
            'plan_id': client['plan_id'],
            'amount': round(random.uniform(8, 30), 2),
            'date': fake.date_this_year(),
            'payment_type' : random.choice(['CARD', 'BANK_TRANSFER', 'PAYPAL', 'CASH'])
        }
        payments.append(payment)
    return payments

def insert_payments(payments):
    print("Insertando pagos...")
    insert_query = """
        INSERT INTO payment (id, id_client, id_plan, amount, date, payment_type)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    data = [(p['id'], p['client_id'], p['plan_id'], p['amount'], p['date'], p['payment_type']) for p in payments]
    cursor.executemany(insert_query, data)
    conn.commit()

def save_to_csv(data, filename):
    print(f"Guardando {filename}...")
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, header=False)

def upload_to_s3(filename):
    print(f"Subiendo {filename} a S3...")
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN
    )
    s3.upload_file(filename, BUCKET_NAME, f"postgres/{filename}")
    print(f"{filename} subido exitosamente.")

def export_plans_to_csv():
    print("Exportando planes a CSV...")
    cursor.execute("SELECT * FROM plan")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv("plans.csv", header=False, index=False)
    upload_to_s3("plans.csv")


if __name__ == "__main__":
    clients = generate_clients()
    insert_clients(clients)
    payments = generate_payments(clients)
    insert_payments(payments)

    save_to_csv(clients, "clients.csv")
    save_to_csv(payments, "payments.csv")
    upload_to_s3("clients.csv")
    upload_to_s3("payments.csv")

    export_plans_to_csv()

    cursor.close()
    conn.close()
    print("Ingesta completa para PostgreSQL.")