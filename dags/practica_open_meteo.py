"""
## Práctica: temperatura histórica de Madrid con Open-Meteo 🌡️

DAG que explota el archivo histórico de [Open-Meteo](https://open-meteo.com/en/docs)
para la ciudad de Madrid (40.4168, -3.7038).

### Flujo

```
  start  →  fetch_temperature  →  calculate_mean  →  send_alert  →  end
```

- **start**: `EmptyOperator` que marca el inicio del DAG.
- **fetch_temperature**: consulta la API para el día de la fecha lógica del run
  (`data_interval_start`) y devuelve la lista de temperaturas por XCom.
- **calculate_mean**: descarta el valor mínimo y el máximo y calcula la media.
- **send_alert**: tarea de alerta con `trigger_rule="all_failed"`; solo se
  ejecuta si algo ha fallado antes. En un caso real enviaría un correo.
- **end**: `EmptyOperator` con `trigger_rule="all_done"`, se ejecuta tanto si
  el DAG termina bien como si falla.

### Detalles

- Programación diaria a las 10:00 hora de Madrid (`0 10 * * *`).
- `catchup=True`: cada run histórico consulta su propio día. El run con fecha
  lógica 28 de octubre consulta el 28 de octubre.
- Reintentos: 3 reintentos (4 intentos en total) con `retry_delay` de 1 minuto.
- `max_active_runs=1` para que el backfill no lance todas las peticiones a la
  API en paralelo.

No requiere credenciales: Open-Meteo es gratuita.
"""

import logging

import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from pendulum import datetime, duration

# --------------- #
# DAG Constants   #
# --------------- #

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
MADRID_LATITUDE = 40.4168
MADRID_LONGITUDE = -3.7038
TIMEZONE = "Europe/Madrid"
TEMPERATURE_FIELD = "temperature_2m"
HTTP_TIMEOUT_SECONDS = 30

log = logging.getLogger(__name__)


# --------------- #
# DAG Definition  #
# --------------- #


@dag(
    start_date=datetime(2026, 10, 1, tz=TIMEZONE),
    schedule="0 10 * * *",
    catchup=True,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "Universidad",
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    tags=["practica", "open-meteo", "ejercicio"],
)
def practica_open_meteo():
    """Temperatura media diaria de Madrid a partir del histórico de Open-Meteo."""

    start = EmptyOperator(task_id="start")

    @task
    def fetch_temperature(**context) -> list[float]:
        """Consulta Open-Meteo para la fecha lógica del run y devuelve las temperaturas."""
        # Fecha lógica del run: cada ejecución consulta su propio día.
        target_date = context["data_interval_start"].date()

        log.info("Consultando temperaturas de Madrid para %s", target_date)

        response = requests.get(
            OPEN_METEO_ARCHIVE_URL,
            params={
                "latitude": MADRID_LATITUDE,
                "longitude": MADRID_LONGITUDE,
                "start_date": target_date.isoformat(),
                "end_date": target_date.isoformat(),
                "hourly": TEMPERATURE_FIELD,
                "timezone": TIMEZONE,
                "temperature_unit": "celsius",
            },
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        response.raise_for_status()

        temperatures = response.json()["hourly"][TEMPERATURE_FIELD]
        log.info("Recibidas %d temperaturas para %s", len(temperatures), target_date)

        # El return se guarda automáticamente en XCom con TaskFlow.
        return temperatures

    @task
    def calculate_mean(temperatures: list[float]) -> float:
        """Media de las temperaturas descartando el mínimo y el máximo."""
        # La API puede devolver null si falta algún dato horario.
        values = [value for value in temperatures if value is not None]

        if len(values) < 3:
            log.warning(
                "Solo hay %d valores válidos; se calcula la media sin descartar extremos",
                len(values),
            )
            mean = sum(values) / len(values)
        else:
            values = sorted(values)[1:-1]  # fuera el mínimo y el máximo
            mean = sum(values) / len(values)

        log.info("Temperatura media de Madrid: %.2f °C", mean)
        return mean

    @task(trigger_rule="all_failed", retries=0)
    def send_alert(**context) -> None:
        """Alerta que solo se ejecuta si alguna tarea anterior ha fallado."""
        run_date = context["data_interval_start"].date()
        message = (
            f"⚠️ El pipeline de temperatura de Madrid ha fallado para el día {run_date}. "
            "Revisa los logs en la UI de Airflow."
        )
        print(message)
        log.error(message)

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    temperatures = fetch_temperature()
    mean = calculate_mean(temperatures)
    alert = send_alert()

    start >> temperatures >> mean >> alert >> end


practica_open_meteo()
