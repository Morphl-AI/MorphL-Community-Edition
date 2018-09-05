from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from flask import (render_template as rt,
                   Flask, request, redirect, url_for, session, jsonify)

from gevent.pywsgi import WSGIServer

class Cassandra:
    def __init__(self):
        self.MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
        self.MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
        self.MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
        self.MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

        self.QUERY = 'SELECT * FROM ga_chu_predictions WHERE client_id = ? LIMIT 1'

        self.CASS_REQ_TIMEOUT = 3600.0

        self.auth_provider = PlainTextAuthProvider(
            username=self.MORPHL_CASSANDRA_USERNAME,
            password=self.MORPHL_CASSANDRA_PASSWORD)
        self.cluster = Cluster(
            [self.MORPHL_SERVER_IP_ADDRESS], auth_provider=self.auth_provider)
        self.session = self.cluster.connect(self.MORPHL_CASSANDRA_KEYSPACE)
        self.session.default_fetch_size = 1

        self.prep_stmt = self.session.prepare(self.QUERY)

    def retrieve_prediction(self, client_id):
        bind_list = [client_id]
        return self.session.execute(self.prep_stmt, bind_list, timeout=self.CASS_REQ_TIMEOUT)._current_rows

app = Flask(__name__)

@app.route('/getprediction/<client_id>')
def get_prediction(client_id):
    p = app.config['CASSANDRA'].retrieve_prediction(client_id)
    p_dict = {'client_id': client_id}
    if len(p) == 0:
        p_dict['error'] = 'N/A'
    else:
        p_dict['result'] = p[0].prediction

    return jsonify(prediction=p_dict)

if __name__ == '__main__':
    app.config['CASSANDRA'] = Cassandra()
    if getenv('DEBUG'):
        app.config['DEBUG'] = True
        flask_port = 5858
        app.run(host='0.0.0.0', port=flask_port)
    else:
        app.config['DEBUG'] = False
        flask_port = 6868
        WSGIServer(('', flask_port), app).serve_forever()
