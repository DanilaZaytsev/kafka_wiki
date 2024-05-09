import numpy as np
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_distances
from transformers import BertTokenizer, BertModel
import torch
import psycopg2

db_params = {
    "host": "master.6af36d96-5c46-4b61-936b-219433f72040.c.dbaas.selcloud.ru",
    "database": "univer",
    "user": "univer",
    "password": "26tcq=0h2f171Ec2K",
    "port": "5433"
}

conn = psycopg2.connect(**db_params)
print("Connected to the PostgreSQL database")

model_name = 'bert-base-multilingual-cased'
tokenizer = BertTokenizer.from_pretrained(model_name)
model = BertModel.from_pretrained(model_name)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

model.to(device)
print(f"Using device: {device}")

def encode_text(text):
    input_ids = tokenizer.encode(text, add_special_tokens=True, max_length=512, truncation=True, padding='max_length')
    input_ids_tensor = torch.tensor([input_ids]).to(device)
    with torch.no_grad():
        outputs = model(input_ids_tensor)
    cls_output = outputs.last_hidden_state[:, 0, :].squeeze().cpu().numpy()
    return cls_output

def load_genre_mapping():
    genre_mapping = {}
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT genre FROM films")
    genres = cursor.fetchall()
    for genre_name, in genres:
        genre_mapping[genre_name] = genre_name
    return genre_mapping

cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM films")
total_films = cursor.fetchone()[0]

# Размер батча для обработки данных
batch_size = 100

genre_mapping = load_genre_mapping()

# Обработка данных по батчам
for offset in range(0, total_films, batch_size):
    # Получаем фильмы батчами
    cursor.execute("SELECT id, plot FROM films ORDER BY id LIMIT %s OFFSET %s", (batch_size, offset))
    films = cursor.fetchall()

    # Получение векторов сюжетов с помощью BERT
    plot_embeddings = np.array([encode_text(plot[1]) for plot in films])

    # Применение косинусной метрики
    cosine_distance = cosine_distances(plot_embeddings)

    # Применение кластеризации
    clustering = AgglomerativeClustering(n_clusters=None, linkage="average", distance_threshold=0.2).fit(cosine_distance)
    cluster_labels = clustering.labels_

    # Пишем в базу
    for i, label in enumerate(cluster_labels):
        film_id = films[i][0]
        genre_name = films[i][1]  # Берем значение жанра из столбца plot
        if genre_name in genre_mapping:
            cursor.execute("UPDATE films SET cluster = %s, genre = %s WHERE id = %s", (int(label), genre_name, film_id))

    conn.commit()
    print(f"Updated database with clustering results for batch {offset + 1}-{offset + batch_size}")

conn.close()
print("Closed database connection")
