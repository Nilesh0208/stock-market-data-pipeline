"""
Basic Airflow health-check DAG for the stock market pipeline.
"""

import socket

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="stock_pipeline_health_check",
    description="Basic execution test for the stock market pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 20, tz="Asia/Kolkata"),
    catchup=False,
    tags=["stock-market", "health-check"],
)
def stock_pipeline_health_check():

    @task
    def start_health_check() -> str:
        message = "Starting stock market pipeline health check"
        print(message)
        return message

    @task
    def check_airflow_environment() -> str:
        hostname = socket.gethostname()

        print("Airflow task execution is working successfully")
        print(f"Airflow container hostname: {hostname}")

        return hostname

    @task
    def finish_health_check() -> str:
        message = "Stock market pipeline health check completed successfully"
        print(message)
        return message

    start = start_health_check()
    environment = check_airflow_environment()
    finish = finish_health_check()

    start >> environment >> finish


stock_pipeline_health_check()