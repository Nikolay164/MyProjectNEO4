создать файла внутри проекта ( в папке с docker-compose.yaml с содержимым: Airflow AIRFLOW_UID=1000 AIRFLOW_GID=0 AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME} AIRFLOW__WEBSERVER__SECRET_KEY=your_random_secret_key_here

PostgreSQL DB_HOST=host.docker.internal DB_NAME=postgres DB_USER=ds DB_PASSWORD=13245678 DB_PORT=5432
