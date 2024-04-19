from transformers import BertTokenizer, BertModel
import torch
import psycopg2
import logging

# больше логирования больше счастья
logging.basicConfig(filename='sql.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

try:
    conn = psycopg2.connect(
        host="master.6af36d96-5c46-4b61-936b-219433f72040.c.dbaas.selcloud.ru",
        database="univer",
        user="univer",
        password="26tcq=0h2f171Ec2K",
        port="5433"
    )
    logging.info("Connected to PostgreSQL database")
except psycopg2.Error as e:
    logging.error("Error connecting to PostgreSQL database: %s", e)

# грузим берт и токенезируемся
model_name = 'google-bert/bert-large-uncased'
tokenizer = BertTokenizer.from_pretrained(model_name)
model = BertModel.from_pretrained(model_name)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)

#енкодинг в модель
def encode_text(text):
    input_ids = tokenizer.encode(text, add_special_tokens=True, max_length=512, truncation=True, padding='max_length')
    input_ids_tensor = torch.tensor([input_ids]).to(device)
    attention_mask = (input_ids_tensor != tokenizer.pad_token_id).long().to(device)
    with torch.no_grad():
        outputs = model(input_ids_tensor, attention_mask=attention_mask)
    cls_output = outputs.last_hidden_state[:, 0, :].squeeze()
    return cls_output

# Функция расчете сходства названия и сюжета
def compute_similarity(title, plot):
    title_embedding = encode_text(title)
    plot_embedding = encode_text(plot)
    similarity = torch.cosine_similarity(title_embedding, plot_embedding, dim=0)
    return similarity.item()

# читаем с базы построчно
try:
    cursor = conn.cursor()
    cursor.execute("SELECT id, title, plot, similarity FROM films")
    films_data = cursor.fetchall()
    logging.info("Прочитал с базы")
except psycopg2.Error as e:
    logging.error("Ошибка чтения: %s", e)

# Апдейтим в базу
for film_data in films_data:
    film_id, title, plot, similarity = film_data
    if similarity is not None:
        logging.info("Для этого фильма уже расчитано сходство/ ID %s", film_id)
        continue

    similarity = compute_similarity(title, plot)
    logging.info("Посчитали сходство для фильма. ID %s: %s", film_id, similarity)
    print(f"Film ID {film_id}: {title} - {plot}")
    print("Similarity:", similarity)

    try:
        cursor.execute("UPDATE films SET similarity = %s WHERE id = %s", (float(similarity), film_id))
        conn.commit()
        logging.info("Обновили запись в базе для фильма ID %s ", film_id)
    except psycopg2.Error as e:
        logging.error("Не смогли записать в базу ID %s: %s", film_id, e)


try:
    conn.close()
    logging.info("Закрыли коннект")
except psycopg2.Error as e:
    logging.error("Не смогли закрыть коннект, чини давай: %s", e)
