from flask import Flask, jsonify
from flask_restful import Api, Resource
from flasgger import Swagger, swag_from

app = Flask(__name__)
api = Api(app)
swagger = Swagger(app)

class HighestRatedByCountry(Resource):
    @app.route('/highest-rated-by-country', methods=['GET'])
    @swag_from({
        'security': [],
        'responses': {
            200: {
                'description': 'A list of films',
                'schema': {
                    'type': 'object',
                    'properties': {
                        'films': {
                            'type': 'array',
                            'items': {
                                'type': 'object',
                                'properties': {
                                    'title': {'type': 'string', 'description': "The film's title"},
                                    'rating': {'type': 'number', 'description': "The film's rating"}
                                }
                            }
                        }
                    }
                }
            }
        }
    })
    def highest_rated_by_country(self):
        """
        File that returns highest rated films by country
        """
        try:
            cur = conn.cursor()
            cur.execute("SELECT country, MAX(imdb_rating) FROM films GROUP BY country")
            data = cur.fetchall()
            result = [{"country": row[0], "max_rating": row[1]} for row in data]
            cur.close()
            return jsonify(result)
        except psycopg2.Error as e:
            error_message = "An error occurred while fetching data: {}".format(e)
            return jsonify({"error": error_message}), 500

api.add_resource(HighestRatedByCountry, '/films/highest_rated_by_country')

if __name__ == '__main__':
    app.run(debug=True)