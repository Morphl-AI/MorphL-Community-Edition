from time import sleep
from os import getenv
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

AIRFLOW_WEB_UI_PASSWORD = getenv('AIRFLOW_WEB_UI_PASSWORD')

user = PasswordUser(models.User())
user.username = 'airflow'
user._set_password = AIRFLOW_WEB_UI_PASSWORD
session = settings.Session()
session.add(user)
session.commit()
sleep(10)
session.close()

