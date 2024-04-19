from flask import Flask, request, render_template_string
import psycopg2

app = Flask(__name__)

# Function to fetch film data from the database
def fetch_films_from_db(query):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host="master.6af36d96-5c46-4b61-936b-219433f72040.c.dbaas.selcloud.ru",
            database="univer",
            user="univer",
            password="26tcq=0h2f171Ec2K",
            port="5433"
        )
        cursor = conn.cursor()

        # Execute the query to fetch film data
        cursor.execute(query)
        films = cursor.fetchall()

        # Close the database connection
        cursor.close()
        conn.close()

        return films
    except Exception as e:
        # Log the exception
        print("Error fetching films from the database:", e)
        return []

@app.route('/')
def index():
    # Render the template with empty films list initially
    template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Film Search</title>
        <style>
            .green {
                color: green;
            }
        </style>
    </head>
    <body>
        <h1>Film Search</h1>
        <form action="/search" method="GET">
            <input type="text" name="query" placeholder="Enter film title...">
            <button type="submit">Search</button>
        </form>
        <br>
        <h2>Search Results:</h2>
        <ul>
            {% for film in films %}
                {% if film[2] > 0.8 %}
                    <li class="green">{{ film[1] }} - Similarity: {{ film[2] }} (The title accurately reflects the plot)</li>
                {% else %}
                    <li>{{ film[1] }} - Similarity: {{ film[2] }}</li>
                {% endif %}
            {% endfor %}
        </ul>
    </body>
    </html>
    """

    # Render the template with an empty films list
    rendered_template = render_template_string(template, films=[])
    return rendered_template

@app.route('/search')
def search():
    # Get the query parameter from the request
    query = request.args.get('query')

    # Fetch films from the database based on the query
    query = f"SELECT id, title, similarity FROM films WHERE title ILIKE '%{query}%'"
    films = fetch_films_from_db(query)

    # Render the template with the fetched films
    template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Film Search</title>
        <style>
            .green {
                color: green;
            }
            .orange {
                color: orange;
            }
            .red {
                color: red;
            }
        </style>
    </head>
    <body>
        <h1>Film Search</h1>
        <form action="/search" method="GET">
            <input type="text" name="query" placeholder="Enter film title...">
            <button type="submit">Search</button>
        </form>
        <br>
        <h2>Search Results:</h2>
        <ul>
            {% for film in films %}
                {% if film[2] > 0.9 %}
                    <li class="green">{{ film[1] }} - Схоство: {{ film[2] }} - в Целом название +- отражает сюжет. НО есть шанс, что просто нет сюжета! Проверь в БД</li>
                {% elif film[2] > 0.8 < 0.9 %}
                    <li class="orange"> {{ film[1] }} - Схоство: {{ film[2] }} - ну так себе, надо бы почитать глазами </li>
                {% elif film[2] < 0.8 %}
                    <li class="red"> {{ film[1] }} - Схоство: {{ film[2] }} - фильм говно по версии BERT </li>
                {% else %}
                    <li>{{ film[1] }} - Similarity: {{ film[2] }}</li>
                {% endif %}
            {% endfor %}
        </ul>
    </body>
    </html>
    """
    rendered_template = render_template_string(template, films=films)
    return rendered_template

if __name__ == '__main__':
    app.run(debug=True)