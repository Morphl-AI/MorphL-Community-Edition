from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory
from cassandra.protocol import ProtocolException

from operator import itemgetter

from flask import (render_template as rt,
                   Flask, request, redirect, url_for, session, jsonify)
from flask_cors import CORS

from gevent.pywsgi import WSGIServer

import jwt
import re
from datetime import datetime

"""
    Database connector
"""


class Cassandra:
    def __init__(self):
        self.MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
        self.MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
        self.MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
        self.MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

        self.CASS_REQ_TIMEOUT = 3600.0

        self.auth_provider = PlainTextAuthProvider(
            username=self.MORPHL_CASSANDRA_USERNAME,
            password=self.MORPHL_CASSANDRA_PASSWORD)
        self.cluster = Cluster(
            [self.MORPHL_SERVER_IP_ADDRESS], auth_provider=self.auth_provider)

        self.session = self.cluster.connect(self.MORPHL_CASSANDRA_KEYSPACE)
        self.session.row_factory = dict_factory
        self.session.default_fetch_size = 100

        self.prepare_statements()

    def prepare_statements(self):
        """
            Prepare statements for database select queries
        """
        self.prep_stmts = {
            'predictions': {},
            'models': {},
            'access_logs': {}
        }

        template_for_single_row = 'SELECT * FROM ga_chp_predictions WHERE client_id = ? LIMIT 1'
        template_for_multiple_rows = 'SELECT client_id, prediction FROM ga_chp_predictions_by_prediction_date WHERE prediction_date = ?'
        template_for_predictions_statistics = 'SELECT loyal, neutral, churning, lost FROM ga_chp_predictions_statistics WHERE prediction_date= ? LIMIT 1'
        template_for_models_rows = 'SELECT accuracy, loss, day_as_str FROM ga_chp_valid_models WHERE is_model_valid = True LIMIT 20 ALLOW FILTERING'
        template_for_access_log_insert = 'INSERT INTO ga_chp_predictions_access_logs (client_id, tstamp, prediction) VALUES (?,?,?)'

        self.prep_stmts['predictions']['single'] = self.session.prepare(
            template_for_single_row)
        self.prep_stmts['predictions']['multiple'] = self.session.prepare(
            template_for_multiple_rows)
        self.prep_stmts['predictions']['statistics'] = self.session.prepare(
            template_for_predictions_statistics)
        self.prep_stmts['models']['multiple'] = self.session.prepare(
            template_for_models_rows)
        self.prep_stmts['access_logs']['insert'] = self.session.prepare(
            template_for_access_log_insert)

    def retrieve_prediction(self, client_id):
        bind_list = [client_id]
        return self.session.execute(self.prep_stmts['predictions']['single'], bind_list, timeout=self.CASS_REQ_TIMEOUT)._current_rows

    def retrieve_predictions(self, paging_state, date):

        bind_list = [date]

        # Check if paginated request
        if paging_state is not None:
            try:
                # Convert page from hex format to bytes
                previous_paging_state = bytes.fromhex(paging_state)
                results = self.session.execute(
                    self.prep_stmts['predictions']['multiple'], bind_list, paging_state=previous_paging_state, timeout=self.CASS_REQ_TIMEOUT)
            except (ValueError, ProtocolException):

                # If paging_state causes an error, return invalid request since the format was probably valid but the actual value was wrong
                return {'status': 0, 'error': 'Invalid pagination request.'}

        else:
            # If no page is set get first page of results
            results = self.session.execute(
                self.prep_stmts['predictions']['multiple'], bind_list, timeout=self.CASS_REQ_TIMEOUT)

        return {
            'status': 1,
            'predictions': results._current_rows,
            'next_paging_state': results.paging_state.hex(
            ) if results.has_more_pages == True else 0
        }

    def get_statistics(self, date):
        bind_list = [date]
        
        response = self.session.execute(
            self.prep_stmts['predictions']['statistics'], bind_list, timeout=self.CASS_REQ_TIMEOUT)._current_rows

        return {} if not response else response[0]

    def get_model_statistics(self):
        return self.session.execute(self.prep_stmts['models']['multiple'], timeout=self.CASS_REQ_TIMEOUT)._current_rows

    def insert_access_log(self, client_id, p):
        bind_list = [client_id, datetime.now(), -1 if len(
            p) == 0 else p[0]['prediction']]

        return self.session.execute(self.prep_stmts['access_logs']['insert'],
                                    bind_list, timeout=self.CASS_REQ_TIMEOUT)


"""
    API class for verifying credentials and handling JWTs.
"""


class API:
    def __init__(self):
        self.API_DOMAIN = getenv('API_DOMAIN')
        self.MORPHL_API_KEY = getenv('MORPHL_API_KEY')
        self.MORPHL_API_JWT_SECRET = getenv('MORPHL_API_JWT_SECRET')

    def verify_jwt(self, token):
        try:
            decoded = jwt.decode(token, self.MORPHL_API_JWT_SECRET)
        except Exception:
            return False

        return (decoded['iss'] == self.API_DOMAIN and
                decoded['sub'] == self.MORPHL_API_KEY)


app = Flask(__name__)
CORS(app)

# @todo Check request origin for all API requests


@app.route("/churning")
def main():
    return "MorphL Predictions API - Churning Users"


@app.route('/churning/getprediction/<client_id>')
def get_prediction(client_id):
    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    # Validate client id (alphanumeric with dots)
    if not re.match('^[a-zA-Z0-9.]+$', client_id):
        return jsonify(status=0, error='Invalid client id.')

    p = app.config['CASSANDRA'].retrieve_prediction(client_id)

    # Log prediction request
    app.config['CASSANDRA'].insert_access_log(client_id, p)

    if len(p) == 0:
        return jsonify(status=0, error='No associated predictions found for that ID.')

    return jsonify(status=1, prediction={'client_id': client_id, 'prediction': p[0]['prediction']})


@app.route('/churning/getpredictions', methods=['GET'], defaults={'client_id': None})
@app.route('/churning/getpredictions/<client_id>', methods=['GET'])
def get_predictions(client_id):

    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    # Check if single prediction request
    if client_id is not None:

        # Validate client id
        if not re.match('^[a-zA-Z0-9.]+$', client_id):
            return jsonify(status=0, error='Invalid client id.')

        prediction = app.config['CASSANDRA'].retrieve_prediction(client_id)

        # Return error if id does not exist in db
        if len(prediction) == 0:
            return jsonify(status=0, error='No associated predictions found for that ID.')

        return jsonify(status=1, predictions=[prediction[0]])

    date = request.args.get('date')
    page = request.args.get('page')

    # Validate date when dealing with multiple predictions request
    if date is None or not re.match('^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$', date):
        return jsonify(status=0, error='Invalid date format.'), 401

    if page is not None and not re.match('^[a-zA-Z0-9_]+$', page):
        return jsonify(status=0, error='Invalid page format.'), 401

    return jsonify(app.config['CASSANDRA'].retrieve_predictions(page, date))


@app.route('/churning/getpredictionsstatistics', methods=['GET'])
def get_predictions_statistics():

    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    date = request.args.get('date')

    # Validate date
    if date is None:
        return jsonify(status=0, error='Missing date.')

    if not re.match('^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$', date):
        return jsonify(status=0, error='Invalid date format.')

    predictions_statistics = app.config['CASSANDRA'].get_statistics(
        date)

    return jsonify(
        status=1,
        predictions_statistics=predictions_statistics,
    )


@app.route('/churning/getmodelstatistics', methods=['GET'])
def get_model_statistics():

    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    model_statistics = app.config['CASSANDRA'].get_model_statistics()

    return jsonify(
        status=1,
        model_statistics=model_statistics
    )


if __name__ == '__main__':
    app.config['CASSANDRA'] = Cassandra()
    app.config['API'] = API()
    if getenv('DEBUG'):
        app.config['DEBUG'] = True
        flask_port = 5858
        app.run(host='0.0.0.0', port=flask_port)
    else:
        app.config['DEBUG'] = False
        flask_port = 6868
        WSGIServer(('', flask_port), app).serve_forever()
