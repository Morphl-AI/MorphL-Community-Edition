airflow_scheduler scheduler &>/dev/null &
airflow_webserver webserver -p 8181 &>/dev/null &
sleep 1

