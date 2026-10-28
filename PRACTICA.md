# Práctica

El objetivo es explotar los datos históricos de "Open-Meteo" para una localización, por ejemplo la ciudad de Madrid. Utilizar grados centígrados y la temperatura (2m) de la fecha lógica del run.

- [Documentación de Open-Meteo](https://open-meteo.com/en/docs).
- **Latitud:** 40.4168° N (40° 25' 0" N).
- **Longitud:** -3.7038° W (3° 42' 14" W).

Petición de ejemplo:

```text
https://archive-api.open-meteo.com/v1/archive?latitude=40.4168&longitude=-3.7038&start_date=2026-10-28&end_date=2026-10-28&hourly=temperature_2m&timezone=Europe/Madrid&temperature_unit=celsius
```

El DAG debería tener los siguientes pasos:

- Tarea inicial con `EmptyOperator()`.
- Tarea que hace las peticiones a la API.
- Tarea que calcula la media de las temperaturas.
- Tarea de alerta (penúltima) con `trigger_rule="all_failed"` y `retries=0`.
- Tarea final con `EmptyOperator()` y `trigger_rule="all_done"`.

Los requisitos técnicos son:

- Utilizar los decoradores `@dag(...)` y `@task(...)` de `airflow.sdk`.
- El DAG debe ejecutarse todos los días a las 10:00 (por ejemplo, `schedule="0 10 * * *"`).
- Especifica la zona horaria `Europe/Madrid`. En la petición a Open-Meteo añade `timezone=Europe/Madrid` (por defecto la API devuelve las horas en GMT) y define el `start_date` del DAG con zona horaria, por ejemplo `datetime(2026, 10, 1, tz="Europe/Madrid")` con `from pendulum import datetime`, para que las 10:00 sean hora de Madrid y no de UTC.
- La fecha de inicio y final será la fecha lógica del run. En la petición, `start_date` y `end_date` serán el mismo día: `context["data_interval_start"].date()`. No uses `datetime.now()`: con `catchup=True`, cada ejecución histórica debe calcular su propia fecha. Así, el run con fecha lógica 28 de octubre consulta el 28.
- La tarea de alerta debe ser la penúltima, con `trigger_rule="all_failed"` y `retries=0`, y soltar un mensaje con `print(...)`. En un caso real enviaría un correo. La tarea final (`EmptyOperator`) debe usar `trigger_rule="all_done"` para ejecutarse tanto si el DAG termina bien como si falla.
- La tarea encargada de hacer la petición a la API de la temperatura debe guardarla en el XCom. Usa `requests.get(url, timeout=30)` y comprueba la respuesta con `response.raise_for_status()`.
- Crear una tarea cuyo propósito es hacer la media para un día de los datos de temperatura, descartando los valores más extremos. Leer el histórico de datos del XCom.
- Establecer una política de reintentos de 3: `retries=3` en `default_args` (o en el `@task` correspondiente), de modo que la tarea se reintente hasta 3 veces después del primer intento (4 intentos en total). Añade también `retry_delay`, por ejemplo `duration(minutes=1)`, para no reintentar de forma inmediata.
- La fecha de inicio del DAG es el 1 de octubre de 2026. Establecer el `catchup` como verdadero.

### Consideraciones

(1) Ejemplo trabajando con fechas lógicas, `data_interval_start`:

Para un DAG diario programado a las 10:00, el run con fecha lógica 28 de octubre de 2026 tiene:

- `data_interval_start` = `2026-10-28 10:00:00` (hora de Madrid; es la fecha que aparece en la UI para ese run)
- día a consultar = `2026-10-28`

Dentro de la tarea:

```python
@task
def fetch_temperature(**context) -> dict:
    run_date = context["data_interval_start"]
    target_date = run_date.date()  # 2026-10-28
    # Usa target_date en start_date y end_date de Open-Meteo
    ...
```

Con `catchup=True`, cada run calcula su propia fecha: el run del 2 de octubre consulta el 2 de octubre, el del 3 consulta el 3, etc.

Recuerda que `data_interval_start` es la fecha lógica del run: el run con fecha lógica 28 de octubre se lanza al terminar su intervalo (29 de octubre a las 10:00), pero la fecha de referencia para la consulta es la lógica (28), así que se consulta el 28.

(2) Aclaración sobre el "empty operator":

El operador vacío (sin lógica) que se usa como punto de entrada o de salida se llama `EmptyOperator` (en versiones anteriores, `DummyOperator`).

```python
from airflow.providers.standard.operators.empty import EmptyOperator

# Dentro de la función decorada con @dag:
start = EmptyOperator(task_id="start")
end = EmptyOperator(task_id="end", trigger_rule="all_done")

fetch = fetch_temperature()    # @task que hace la petición
mean = calculate_mean(fetch)   # @task que calcula la media
alert = send_alert()           # definida con @task(trigger_rule="all_failed", retries=0)

start >> fetch >> mean >> alert >> end
```

`EmptyOperator` no ejecuta ninguna lógica: solo marca el inicio/fin del DAG y sirve para agrupar tareas en el grafo y facilitar la legibilidad del mismo. Se conecta como cualquier otra tarea con `>>`. En Airflow 3 se importa desde `airflow.providers.standard.operators.empty`; en Airflow 2 se importaba desde `airflow.operators.empty` (en Airflow 3 esta última ruta sigue funcionando, pero está deprecada).

La tarea final usa `trigger_rule="all_done"` para ejecutarse tanto si el DAG termina bien como si falla; si no, al saltarse la tarea de alerta en los runs correctos, también se saltaría la tarea final.

(3) Ejemplo de uso de XCom:

Con TaskFlow, el `return` de una tarea se guarda automáticamente en XCom y la tarea siguiente lo recibe como argumento:

```python
@task
def fetch_temperature(**context) -> list[float]:
    target_date = context["data_interval_start"].date()
    response = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": 40.4168,
            "longitude": -3.7038,
            "start_date": target_date.isoformat(),
            "end_date": target_date.isoformat(),
            "hourly": "temperature_2m",
            "timezone": "Europe/Madrid",
            "temperature_unit": "celsius",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["hourly"]["temperature_2m"]  # XCom automático


@task
def calculate_mean(temperatures: list[float]) -> float:
    temperatures = sorted(temperatures)[1:-1]  # ejemplo: quita el mínimo y el máximo
    return sum(temperatures) / len(temperatures)
```

En el cuerpo del DAG, la dependencia y el paso por XCom se crean así:

```python
temperatures = fetch_temperature()
mean = calculate_mean(temperatures)
```

Si prefieres usar una clave explícita en XCom:

```python
@task
def fetch_temperature(**context) -> None:
    # ... misma petición que en el ejemplo anterior ...
    temperatures = response.json()["hourly"]["temperature_2m"]
    context["ti"].xcom_push(key="temperatures", value=temperatures)


@task
def calculate_mean(**context) -> float:
    temperatures = context["ti"].xcom_pull(
        task_ids="fetch_temperature",
        key="temperatures",
    )
    temperatures = sorted(temperatures)[1:-1]
    return sum(temperatures) / len(temperatures)
```

`xcom_pull` lee el XCom de la ejecución actual. Para leer el de ejecuciones anteriores se usa `include_prior_dates=True`, pero en este ejercicio no hace falta.

(4) Backfill: hasta unas 27 ejecuciones:

Con `start_date=2026-10-01`, `schedule="0 10 * * *"` y `catchup=True`, al activar el DAG el 28 de octubre Airflow crea de golpe las ejecuciones cuyo intervalo ya ha terminado: una por cada día del 1 al 27 de octubre, en total unas 27 ejecuciones. No es un error; es el backfill. Si lo activas más adelante, el número crece.

Cada ejecución hace una petición a la API. Para que no se ejecuten todas en paralelo, limita la concurrencia:

```python
@dag(
    start_date=datetime(2026, 10, 1, tz="Europe/Madrid"),
    schedule="0 10 * * *",
    catchup=True,
    max_active_runs=1,
)
def practica():
    ...
```

`max_active_runs=1` no evita que se creen las ejecuciones, pero hace que se procesen de una en una. También puedes pausar el DAG, lanzar solo el run que te interese y activarlo después.
