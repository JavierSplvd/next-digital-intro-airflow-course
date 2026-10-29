"""Práctica: calcula la temperatura media diaria de Madrid con Open-Meteo."""

import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from pendulum import datetime, duration

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
MADRID_LATITUDE = 40.4168
MADRID_LONGITUDE = -3.7038
TIMEZONE = "Europe/Madrid"


@dag(
    dag_id="practica_open_meteo",
    start_date=datetime(2026, 10, 1, tz=TIMEZONE),
    schedule="0 10 * * *",
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "Universidad",
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    tags=["practica", "open-meteo"],
    is_paused_upon_creation=True,
)
def practica_open_meteo():
    start = EmptyOperator(task_id="start")

    @task
    def fetch_temperature(**context) -> list[float]:
        target_date = context["data_interval_start"].date()
        response = requests.get(
            OPEN_METEO_URL,
            params={
                "latitude": MADRID_LATITUDE,
                "longitude": MADRID_LONGITUDE,
                "start_date": target_date.isoformat(),
                "end_date": target_date.isoformat(),
                "hourly": "temperature_2m",
                "timezone": TIMEZONE,
                "temperature_unit": "celsius",
            },
            timeout=30,
        )
        response.raise_for_status()

        temperatures = response.json()["hourly"]["temperature_2m"]
        if not temperatures:
            raise ValueError(f"Open-Meteo no devolvió temperaturas para {target_date}.")

        return temperatures

    @task
    def calculate_mean(temperatures: list[float]) -> float:
        if len(temperatures) < 3:
            raise ValueError(
                "Se necesitan al menos tres temperaturas para descartar "
                "el mínimo y el máximo."
            )

        trimmed_temperatures = sorted(temperatures)[1:-1]
        mean = sum(trimmed_temperatures) / len(trimmed_temperatures)
        print(f"Temperatura media de Madrid (sin extremos): {mean:.2f} °C")
        return mean

    @task(trigger_rule="all_failed", retries=0)
    def send_alert() -> None:
        print("Alerta: han fallado las tareas de obtención o cálculo de temperatura.")

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    temperatures = fetch_temperature()
    mean = calculate_mean(temperatures)
    alert = send_alert()

    start >> temperatures >> mean >> alert >> end


practica_open_meteo()
