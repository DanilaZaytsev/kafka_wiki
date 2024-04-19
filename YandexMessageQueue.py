import requests
import boto3
from bs4 import BeautifulSoup

# Параметры YMQ
YMQ_ENDPOINT = "https://message-queue.api.cloud.yandex.net/b1gvipuih3l7p0gmqj05/dj600000001h0s1n07md/links"
YMQ_QUEUE_NAME = "links"
YMQ_REGION = "ru-central1"

def get_film_links(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        film_links = soup.select(".mw-category-group ul li a")
        return ["https://ru.wikipedia.org" + link.get("href") for link in film_links if link.get("href")]
    else:
        print("Не удалось получить доступ к", url)
        return []

def get_all_category_pages(base_url):
    category_pages = []
    response = requests.get(base_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        category_pages.append(base_url)
        # Поиск ссылок на другие страницы категории
        next_page_link = soup.find('a', string='Следующая страница')
        while next_page_link:
            next_page_url = "https://ru.wikipedia.org" + next_page_link.get('href')
            category_pages.append(next_page_url)
            response = requests.get(next_page_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            next_page_link = soup.find('a', string='Следующая страница')
    return category_pages

def main():
    # Create boto3 SQS client (configured for YMQ endpoint)
    client = boto3.client(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1',
        aws_access_key_id='YF',
        aws_secret_access_key='YCR'
    )

    # URL-адрес категории Wikipedia
    base_category_url = "https://ru.wikipedia.org/wiki/Категория:Телефильмы_по_алфавиту"

    # Получаем ссылки на все страницы категории
    category_pages = get_all_category_pages(base_category_url)

    # Извлекаем ссылки на фильмы с каждой страницы категории
    for page_url in category_pages:
        film_links = get_film_links(page_url)
        # Отправляем ссылки в YMQ (using SQS methods)
        for link in film_links:
            response = client.send_message(
                QueueUrl=f"{YMQ_ENDPOINT}/{YMQ_QUEUE_NAME}",  # YMQ-specific queue URL
                MessageBody=link
            )
            print(f"Ссылка отправлена в YMQ: {link}")

if __name__ == "__main__":
    main()