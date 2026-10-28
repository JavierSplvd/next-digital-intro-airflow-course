# Practica

El objetivo es explotar los datos de "open meteo" para una localizacion, por ejemplo la ciudad de Madrid. Utilizar grados centigrados, prediccion a 1 dia, temperatura (2m).
- [Documentacion de "open meteo"](https://open-meteo.com/en/docs).
- **Latitud:** 40.4168° N (40° 25' 0" N).
- **Longitud:** -3.7038° W (3° 42' 14" W).

Peticion de ejemplo:
```
https://api.open-meteo.com/v1/forecast?latitude=40.4168&longitude=-3.7038&hourly=temperature_2m&timezone=Europe/Madrid&start_date=2026-10-28&end_date=2026-10-28
```

El DAG deberia tener los siguientes pasos:
- Tarea inicial con `EmptyOperator()`.
- Tarea que hace las peticiones a la API.
- Tarea que calcula la media de las temperaturas.
- Tarea que envia el mensaje de alerta en caso de que fallen.
- Tarea final con `EmptyOperator()`.

Los requisitos tecnicos son:
- Utilizar decoradores `@Dag(...)` y `@Task(...)`.
- El DAG debe ejecutarse todos los dias a las 10:00AM.
- Especifica la zona horaria `Europe/Madrid`. En la petición a Open-Meteo añade `timezone=Europe/Madrid` (por defecto la API devuelve las horas en GMT) y define el `start_date` del DAG con zona horaria, por ejemplo `datetime(2026, 10, 1, tz="Europe/Madrid")` con `from pendulum import datetime`, para que las 10:00 sean hora de Madrid y no de UTC.
- La fecha de inicio y final debe ser la del día anterior. En la petición, `start_date` y `end_date` serán el mismo día (la fecha de la ejecución menos un día). Dentro de la tarea usa `context["data_interval_start"]` (la fecha lógica del run que ves en la UI) y réstale un día con `timedelta(days=1)`; no uses `datetime.now()`, porque con `catchup=True` cada ejecución histórica debe calcular su propia fecha.
- Debe existir una tarea final con un "trigger" de `all_failed` y que suelta un mensaje de alerta. En un caso real esto enviaria un correo pero por simplicidad lo dejamos en un `print(...)`.
- La tarea encargada de hacer la peticion a la API de la temperatura debe guardarla en el XCOM.
- Crear una tarea cuyo proposito es hacer la media para un dia de los datos de temperatura, descartando los valores mas extremos. Leer el historico de datos del XCOM.
- Establecer una política de reintentos de 3: `retries=3` en `default_args` (o en el `@task` correspondiente), de modo que la tarea se reintente hasta 3 veces después del primer intento (4 intentos en total). Añade también `retry_delay`, por ejemplo `duration(minutes=1)`, para no reintentar de forma inmediata.
- La fecha de inicio del DAG es el 1 de octubre de 2026. Establecer el `catchup`como verdadero.

### Consideraciones

Ejemplo trabajando con fechas logicas, `data_interval_start`:

Para un DAG diario programado a las 10:00, el run con fecha lógica 28 de octubre de 2026 tiene:

- `data_interval_start` = `2026-10-28 10:00:00` (hora de Madrid; es la fecha que aparece en la UI para ese run)
- día a consultar = `2026-10-27`

Dentro de la tarea:

```python
from datetime import timedelta

@task
def fetch_temperature(**context) -> dict:
    run_date = context["data_interval_start"]
    target_date = (run_date - timedelta(days=1)).date()  # 2026-10-27
    # Usa target_date en start_date y end_date de Open-Meteo
    ...
```

Con `catchup=True`, cada run calcula su propia fecha: el run del 2 de octubre consulta el 1 de octubre, el del 3 consulta el 2, etc.

Aclaración sobre el "empty operator":

El operador vacío (sin lógica) que se usa como punto de entrada o de salida se llama `EmptyOperator` (en versiones anteriores, `DummyOperator`).

```python
from airflow.providers.standard.operators.empty import EmptyOperator

# Dentro de la función decorada con @dag:
start = EmptyOperator(task_id="start")
end = EmptyOperator(task_id="end")

fetch = fetch_temperature()    # @task que hace la petición
mean = calculate_mean(fetch)   # @task que calcula la media

start >> fetch >> mean >> end
# La tarea de alerta se conecta aparte con su trigger_rule.
```

`EmptyOperator` no ejecuta ninguna lógica: solo marca el inicio/fin del DAG y sirve para agrupar tareas en el grafo y facilitar la legibilidad del mismo. Se conecta como cualquier otra tarea con `>>`. En Airflow 3 se importa desde `airflow.providers.standard.operators.empty`; en Airflow 2 se importaba desde `airflow.operators.empty` (en Airflow 3 esta última ruta sigue funcionando, pero está deprecada).