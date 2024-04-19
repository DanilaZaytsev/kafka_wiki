import requests
import boto3
from bs4 import BeautifulSoup
import json
from multiprocessing import Process
import re

# Параметры YMQ
YMQ_ENDPOINT_UNP = "https://message-queue.api.cloud.yandex.net/b1gvipuih3l7p0gmqj05/dj600000001h0s1n07md/links" #ссыль на очередь с сылками
YMQ_ENDPOINT_PR = "https://message-queue.api.cloud.yandex.net/b1gvipuih3l7p0gmqj05/dj600000001h0uip07md/parsed" #ссыль на очередь куда парспарсеное сообщение писать
YMQ_REGION = "ru-central1" #регион из настроек яндекс
UNPROCESSED_QUEUE_NAME = "links"
PARSED_QUEUE_NAME = "parsed"

def parse_and_send_message(url, client):
    try:
        # Парсинг данных
        film_data = get_film_data(url)
        if film_data:
            client.send_message(
                QueueUrl=f"{YMQ_ENDPOINT_PR}",
                MessageBody=json.dumps(film_data)
            )
            print(f"Фильм {film_data['title']} отправлен в parsed")
        else:
            print(f"Ошибка при парсинге {url}")

    except Exception as e:
        print(f"Ошибка при обработке URL {url}: {e}")

def get_film_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        section = soup.find(id='Сюжет')
        if section:
            next_paragraph = section.find_next('p')
            if next_paragraph:
                plot_text = next_paragraph.get_text()
            else:
                plot_text = "Сюжет не найден"
        else:
            plot_text = "Сюжет не найден"

        infobox_table = soup.find('table', class_='infobox')
        if infobox_table:
            content_type = infobox_table.get('data-name')
        else:
            content_type = "Инфа о типе контента не найдена"
        genres = []  # Создаем пустой список для жанров
        if infobox_table:
            genre_spans = infobox_table.find_all('span', {'data-wikidata-property-id': 'P136'})
            for span in genre_spans:
                # Ищем все ссылки внутри каждого span и получаем текст жанра
                genre_texts = [a.get_text(strip=True) for a in span.find_all('a')]
                # Добавляем тексты жанров в список genres
                genres.extend(genre_texts)


        #year_element = soup.find('th', string='Год')
        #if year_element:
        #    year = year_element.find_next('span', class_='dtstart')
        #    if year:
        #        year = year.text.strip()
        #    else:
        #        year = "Год не найден"
        #else:
        #    year = "Год не найден"

        release_dates = get_release_date(soup)

        # Объединяем список жанров в одну строку с разделителем ", "
        genres_str = ", ".join(genres)
        production_company = ""
        countries = ""
        imdb_id = ""
        imdb_rating = ""

        studio = get_studio(soup)
        #if infobox_table:
        #    company_row = infobox_table.find('th', string=['Кинокомпания', 'Студия', 'Студии', 'Издатель'])
        #    if company_row:
        #        company_cell = company_row.find_next_sibling('td')
        #        if company_cell:
        #            company_links = company_cell.find_all('a')
        #            if company_links:
        #                production_company = ', '.join(link.text.strip() for link in company_links)
        #            else:
        #                production_company = company_cell.get_text(strip=True)

        country_element = soup.find('th', string='Страны') or soup.find('th', string='Страна')
        if country_element:
            country_td = country_element.find_next('td')
            country_links = country_td.find_all('a', title=True)
            if country_links:
                countries = [country['title'] for country in country_links]
            else:
                countries = [country_td.get_text(strip=True)]
        imdb_element = soup.find('span', {'data-wikidata-property-id': 'P345'})
        if imdb_element:
            imdb_link = imdb_element.find('a')
            if imdb_link:
                imdb_id = imdb_link.get('href').split('/')[-2]
                imdb_rating = get_imdb_rating(imdb_id)
        film_data = {
            "title": soup.find("h1", class_="firstHeading").text,
            "plot": plot_text,
            "genre": genres_str,
            "content_type": content_type,
            "product_company": studio,
            "country": ','.join(countries),
            "year": release_dates,
            "imdb_id": imdb_id if imdb_id else "Не найдено",
            "imdb_rating": imdb_rating if imdb_rating is not None else "Рейтинга нет"
        }
        return film_data
    else:
        print(f"Не удалось получить доступ к {url}")
        return None

def get_imdb_rating(imdb_id):
    url = f"https://www.imdb.com/title/{imdb_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.5"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Бросает исключение, если получен неверный статус ответа
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            script = soup.find('script', id='__NEXT_DATA__')
            if script:
                data = json.loads(script.contents[0])
                ratings_summary = data['props']['pageProps']['aboveTheFoldData']['ratingsSummary']
                rating_value = ratings_summary['aggregateRating']
                print("IMDb Rating Value:", rating_value)  # Добавим вывод
                if rating_value is not None:  # Проверка наличия рейтинга
                    return float(rating_value)
                else:
                    print("Рейтинг IMDb не найден")
            else:
                print("Скрипт с данными не найден")
        else:
            print(f"Ошибка при получении страницы: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Произошла ошибка при отправке запроса: {e}")
    return None

def get_release_date(soup):
    release_date_th = soup.find('th', string='Дата выхода')
    if release_date_th:
        release_date_td = release_date_th.find_next_sibling('td')
        if release_date_td:
            release_date_spans = release_date_td.find_all('span', class_='no-wikidata')
            for span in release_date_spans:
                release_date = span.get_text(strip=True)
                year = extract_year_from_date(release_date)
                if year:
                    return year
    return "Год не найден"

def extract_year_from_date(date):
    year_match = re.search(r'\b\d{4}\b', date)
    if year_match:
        return year_match.group()
    return None

def get_studio(soup):
    studio = ""
    studio_th = soup.find('th', string=lambda text: text and 'Студи' in text)
    if studio_th:
        studio_td = studio_th.find_next_sibling('td')
        if studio_td:
            studio_span = studio_td.find('span', class_='no-wikidata')
            if studio_span:
                studio = studio_span.get_text(strip=True)
    if not studio:
        studio = "Не найдено"
    return studio

def main():
    # Бото3 клиент
    client = boto3.client(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1',
        aws_access_key_id='Y',  # Замените на ваш ключ
        aws_secret_access_key='Yzd-R'  # Замените на ваш ключ
    )

    # Получение сообщения из очереди flim_links
    response = client.receive_message(
        QueueUrl=f"{YMQ_ENDPOINT_UNP}",
        MaxNumberOfMessages=2,  # Вычитываем по n сообещний из очереди и парсим сразу в n сообщений
        WaitTimeSeconds=10
    )

    if 'Messages' not in response or len(response['Messages']) == 0:
        print("Очередь пуста")
        exit()

    messages = response['Messages']
    processes = []

    # Создание воркеров для парсинга и отправки данных
    for message in messages:
        link = message['Body']
        p = Process(target=parse_and_send_message, args=(link, client))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    # Удаление сообщений из очереди
    for message in messages:
        receipt_handle = message['ReceiptHandle']
        client.delete_message(
            QueueUrl=f"{YMQ_ENDPOINT_UNP}",
            ReceiptHandle=receipt_handle
        )

if __name__ == "__main__":
    while True:
        main()