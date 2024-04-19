import json
import requests
import boto3
from psycopg2 import connect  # Import psycopg2 for connecting to Postgres
from multiprocessing import Process

# параметры для  YMQ
YMQ_ENDPOINT = "https://message-queue.api.cloud.yandex.net/b1gvipuih3l7p0gmqj05/dj600000001h0uip07md/parsed"
YMQ_REGION = "ru-central1"
PARSED_QUEUE_NAME = "parsed"

# параметры постгри
POSTGRES_HOST = "master.6af36d96-5c46-4b61-936b-219433f72040.c.dbaas.selcloud.ru"
POSTGRES_PORT = 5433
POSTGRES_DATABASE = "univer"
POSTGRES_USER = "univer"
POSTGRES_PASSWORD = "26tcq=0h2f171Ec2K"


def process_message(message):
    # подключаемся к постгри
    try:
        connection = connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DATABASE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = connection.cursor()


        data = json.loads(message['Body'])

        #проверка imdb что есть рейтинг, если нет, отправляем базу 0.0 потому что float на таблице
        imdb_rating = data['imdb_rating']
        if imdb_rating ==  "Рейтинга нет" or imdb_rating == "":
            imdb_rating = 0.0

        #провека года на валидность
        year = data['year']
        if year == "Год не найден" or year == "":
            year = 0000

        sql = """
            INSERT INTO films (
                title, plot, genre, content_type, product_company, country, imdb_id, imdb_rating, year
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            data['title'],
            data['plot'],
            data['genre'],
            data['content_type'],
            data['product_company'],
            data['country'],
            data['imdb_id'],
            imdb_rating,
            year
        ))
        connection.commit()


        print(f"Фильм {data['title']} добавлен в базу")

    except Exception as e:
        print(f"Ошибка при работе с базой данных: {e}")

    finally:
        # Close connection if it exists
        if connection:
            connection.close()

def worker():
    while True:
        # читаем сообщение из парсед_дата
        response = client.receive_message(
            QueueUrl=f"{YMQ_ENDPOINT}",
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        if 'Messages' not in response or len(response['Messages']) == 0:
            print("Очедь пуста")
            exit()

        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']

        process_message(message)

        # Удаляем сообщение из парсед дата
        client.delete_message(
            QueueUrl=f"{YMQ_ENDPOINT}",
            ReceiptHandle=receipt_handle
        )

if __name__ == "__main__":
    #бото3 клиент
    client = boto3.client(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1',
        aws_access_key_id='YCKJvYZF',  # Replace with placeholder
        aws_secret_access_key='Y'  # Replace with placeholder
    )

    processes = []
    for _ in range(5):
        p = Process(target=worker)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()