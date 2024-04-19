import numpy as np
from sklearn.cluster import MiniBatchKMeans
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

# грузим модель берт, токенеризруем и проверяем доступна ли видяха
model_name = 'bert-base-multilingual-cased'
tokenizer = BertTokenizer.from_pretrained(model_name)
model = BertModel.from_pretrained(model_name)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

#запихиваем в видеокарту
model.to(device)
print(f"Using device: {device}")

# Магия 1
def encode_text(text):
    input_ids = tokenizer.encode(text, add_special_tokens=True, max_length=512, truncation=True, padding='max_length')
    input_ids_tensor = torch.tensor([input_ids]).to(device)
    with torch.no_grad():
        outputs = model(input_ids_tensor)
    cls_output = outputs.last_hidden_state[:, 0, :].squeeze().cpu().numpy()
    return cls_output

# Общее колво фильмов
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM films")
total_films = cursor.fetchone()[0]

# батч
batch_size = 100

# Еще одна магия
for offset in range(0, total_films, batch_size):
    # Читаем фильмы батчами
    cursor.execute("SELECT id, plot FROM films ORDER BY id LIMIT %s OFFSET %s", (batch_size, offset))
    films = cursor.fetchall()

    # Векторизуемся
    plot_embeddings = np.array([encode_text(plot[1]) for plot in films])

    # Применяем minibatch алгорится для кластеризации
    kmeans = MiniBatchKMeans(n_clusters=5, batch_size=batch_size)
    kmeans.fit(plot_embeddings)
    cluster_labels = kmeans.labels_

    # Вывод результат и пишем в базу
    for i, label in enumerate(cluster_labels):
        film_id = films[i][0]
        cursor.execute("UPDATE films SET cluster = %s WHERE id = %s", (int(label), film_id))

    conn.commit()
    print(f"Updated database with clustering results for batch {offset + 1}-{offset + batch_size}")

conn.close()
print("Closed database connection")
