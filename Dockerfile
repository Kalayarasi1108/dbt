FROM python:3.10.5

WORKDIR /

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . .

COPY  /dags /root/airflow/dags
# path destination

RUN dbt clean --project-dir /

RUN dbt deps --project-dir /

RUN airflow db init



ENV cmd="airflow webserver"

CMD ["/bin/bash", "-c", "${cmd}"]

EXPOSE 8080