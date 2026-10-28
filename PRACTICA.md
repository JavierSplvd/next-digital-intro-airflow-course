El objetivo es explotar los datos de "open meteo" para una localizacion, por ejemplo la ciudad de Madrid. Utilizar grados centigrados, prediccion a 1 dia, temperatura (2m).
- **Latitud:** 40.4168° N (40° 25' 0" N)
- **Longitud:** -3.7038° W (3° 42' 14" W) 

Los requisitos tecnicos son:
- Utilizar decoradores `@Dag(...)` y `@Task(...)`.
- El DAG debe ejecutarse todos los dias a las 10:00AM.
- La fecha de inicio y final debe ser la del dia anterior. Esto es que en la peticion el `start_date` y el `end_date`sera la fecha de la ejecucion menos un dia.
- Debe existir una tarea final con un "trigger" de `all_failed` y que suelta un mensaje de alerta. En un caso real esto enviaria un correo pero por simplicidad lo dejamos en un `print(...)`.
- La tarea encargada de hacer la peticion a la API de la temperatura debe guardarla en el XCOM.
- Crear una tarea cuyo proposito es hacer la media para un dia de los datos de temperatura, descartando los valores mas extremos. Leer el historico de datos del XCOM.
- Establecer una politica de reintentos de 3.
- La fecha de inicio del DAG es el 1 de octubre de 2026. Establecer el `catchup`como verdadero.

Peticion de ejemplo:
```
https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m&start_date=2026-10-28&end_date=2026-10-28
```