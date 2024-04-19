import boto3

# Параметры YMQ
YMQ_ENDPOINT = "https://message-queue.api.cloud.yandex.net/b1gvipuih3l7p0gmqj05/dj600000001h0uip07md/parsed"  # Замените на свою очередь
YMQ_REGION = "ru-central1"  # Замените на свой регион

def receive_messages(queue_url, max_messages=3):
    # Создаем клиента SQS
    client = boto3.client(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name=YMQ_REGION,
        aws_access_key_id='Y',  # Замените на ваш ключ
        aws_secret_access_key=''  # Замените на ваш ключ
    )

    # Получаем сообщения из очереди
    response = client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=5
    )

    # Выводим сообщения в консоль
    if 'Messages' in response:
        for message in response['Messages']:
            print("Сообщение:")
            print(message['Body'])
            print()

if __name__ == "__main__":
    receive_messages(YMQ_ENDPOINT)