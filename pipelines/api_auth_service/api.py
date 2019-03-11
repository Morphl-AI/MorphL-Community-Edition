from os import getenv

from flask import (Flask, request, jsonify)
from flask_cors import CORS

from gevent.pywsgi import WSGIServer

import jwt
from datetime import datetime, timedelta

"""
    Database connector
"""


"""
    API class for verifying credentials and handling JWTs.
"""


class API:
    def __init__(self):
        self.API_DOMAIN = getenv('API_DOMAIN')
        self.MORPHL_DASHBOARD_USERNAME = getenv('MORPHL_DASHBOARD_USERNAME')
        self.MORPHL_DASHBOARD_PASSWORD = getenv('MORPHL_DASHBOARD_PASSWORD')
        self.MORPHL_API_KEY = getenv('MORPHL_API_KEY')
        self.MORPHL_API_SECRET = getenv('MORPHL_API_SECRET')
        self.MORPHL_API_JWT_SECRET = getenv('MORPHL_API_JWT_SECRET')

        # Set JWT expiration date at 30 days
        self.JWT_EXP_DELTA_DAYS = 30

    def verify_login_credentials(self, username, password):
        return username == self.MORPHL_DASHBOARD_USERNAME and password == self.MORPHL_DASHBOARD_PASSWORD

    def verify_keys(self, api_key, api_secret):
        return api_key == self.MORPHL_API_KEY and api_secret == self.MORPHL_API_SECRET

    def generate_jwt(self):
        payload = {
            'iss': self.API_DOMAIN,
            'sub': self.MORPHL_API_KEY,
            'iat': datetime.utcnow(),
            'exp': datetime.utcnow() + timedelta(days=self.JWT_EXP_DELTA_DAYS),
        }

        return jwt.encode(payload, self.MORPHL_API_JWT_SECRET, 'HS256').decode('utf-8')

    def verify_jwt(self, token):
        try:
            decoded = jwt.decode(token, self.MORPHL_API_JWT_SECRET)
        except Exception:
            return False

        return (decoded['iss'] == self.API_DOMAIN and
                decoded['sub'] == self.MORPHL_API_KEY)


app = Flask(__name__)
CORS(app)


@app.route("/")
def main():
    return "MorphL Predictions API"


@app.route('/authorize', methods=['POST'])
def authorize():

    if request.form.get('api_key') is None or request.form.get('api_secret') is None:
        return jsonify(error='Missing API key or secret')

    if app.config['API'].verify_keys(
            request.form['api_key'], request.form['api_secret']) == False:
        return jsonify(error='Invalid API key or secret')

    return jsonify(token=app.config['API'].generate_jwt())


@app.route("/dashboard/login", methods=['POST'])
def authorize_login():

    if request.form.get('username') is None or request.form.get('password') is None:
        return jsonify(status=0, error='Missing username or password.')

    if not app.config['API'].verify_login_credentials(request.form['username'], request.form['password']):
        return jsonify(status=0, error='Invalid username or password.')

    return jsonify(status=1, token=app.config['API'].generate_jwt())


@app.route("/dashboard/verify-token", methods=['GET'])
def verify_token():

    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error="Token invalid.")
    return jsonify(status=1)


if __name__ == '__main__':
    app.config['API'] = API()
    if getenv('DEBUG'):
        app.config['DEBUG'] = True
        flask_port = 5858
        app.run(host='0.0.0.0', port=flask_port)
    else:
        app.config['DEBUG'] = False
        flask_port = 6868
        WSGIServer(('', flask_port), app).serve_forever()
