"""
## Ejercicio: Pipeline de películas 🎬

Pipeline incompleto para practicar
Usa la API gratuita de OMDb (https://www.omdbapi.com/) para buscar películas.

### Instrucciones

1. Obtén una API key gratuita en: https://www.omdbapi.com/apikey.aspx
2. Copia `.env.example` a `.env` y pon tu key en `OMDB_API_KEY`.
3. Revisa las funciones para entender el flujo.
4. Ejecuta el DAG y verifica que funciona en la UI de Airflow.

### Flujo del pipeline:

```
  fetch_movie_data  →  transform_movie_info  →  display_results
```

### Conceptos que practicarás:
- Llamadas a APIs externas con requests
- Transformación de datos con diccionarios
- Paso de datos entre tareas con XCom (TaskFlow API)
- Acceso seguro a diccionarios con .get()
- Logging con el módulo logging

### Documentación oficial de Airflow

- [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [XCom](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
- [Conceptos: DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
- [Logging en tareas](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/logging-tasks.html)
"""

import logging
import os
from pathlib import Path

import requests
from airflow.sdk import dag, task
from dotenv import load_dotenv
from pendulum import datetime, duration

# --------------- #
# DAG Constants   #
# --------------- #

# Carga el .env de la raíz del proyecto (dags/ -> raíz).
# En Astro CLI el .env ya se inyecta como variable de entorno en el
# contenedor, así que aquí solo se usa cuando ejecutas fuera de él.
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

OMDB_API_URL = "https://www.omdbapi.com/"
OMDB_API_KEY = os.getenv("OMDB_API_KEY")

if not OMDB_API_KEY:
    raise ValueError(
        "Falta la variable de entorno OMDB_API_KEY. "
        "Copia .env.example a .env y pon tu API key de OMDb."
    )

HTTP_TIMEOUT_SECONDS = 15

# Lista de películas a buscar
# Nota: la API de OMDb usa títulos en inglés.
MOVIES_TO_SEARCH: list[str] = [
    "Wuthering Heights",
    "Ídolos",
    "Oppenheimer",
    "Inside Out 2",
    "Dune: Part Two",
    "The Wild Robot",
    "Everything Everywhere All at Once",
    "Emilia Pérez",
]

log = logging.getLogger(__name__)


def _to_float(value) -> float:
    """Convierte un valor de la API a float.

    OMDb devuelve "N/A" (string) cuando un dato no existe, por ejemplo en
    películas sin puntuación registrada. En ese caso devolvemos 0.0.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


# --------------- #
# DAG Definition  #
# --------------- #


@dag(
    start_date=datetime(2025, 4, 1),
    schedule=None,  # Solo ejecución manual
    doc_md=__doc__,
    default_args={
        "owner": "Universidad",
        "retries": 2,
        "retry_delay": duration(seconds=5),
    },
    tags=["ejercicio", "educativo", "peliculas"],
    is_paused_upon_creation=True,  # El DAG se crea pausado; hay que activarlo manualmente en la UI
    catchup=False,  # No ejecuta runs pasados; solo programa desde ahora en adelante
)
def exercise_movie_pipeline():
    """Pipeline de películas para completar"""

    @task
    def fetch_movie_data() -> list[dict]:
        """
        Busca información de cada película en la API de OMDb.

        Llama a la API de OMDb para cada título.
        - Usa requests.get() con los parámetros: t=titulo, apikey=OMDB_API_KEY
        - Verifica que la respuesta sea exitosa con response.raise_for_status()
        - Añade el JSON de la respuesta a la lista de resultados
        - Usa log.info() para registrar cada película encontrada

        Ejemplo de URL: https://www.omdbapi.com/?t=Inception&apikey=TU_KEY
        """
        movie_results: list[dict] = []

        for movie_title in MOVIES_TO_SEARCH:
            # Usamos params= para que requests codifique títulos con espacios
            # o acentos (ej. "Dune: Part Two") de forma segura en la URL.
            response = requests.get(
                OMDB_API_URL,
                params={"t": movie_title, "apikey": OMDB_API_KEY},
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            # Lanza una excepción si la API responde 4xx/5xx (ej. key inválida).
            response.raise_for_status()
            movie_data = response.json()

            # OMDb responde 200 con {"Response": "False"} cuando no encuentra
            # la película (títulos en español o con otro nombre oficial).
            if movie_data.get("Response") == "False":
                log.warning(
                    "No se encontró %r: %s",
                    movie_title,
                    movie_data.get("Error", "error desconocido"),
                )
                continue

            log.info(
                "Película encontrada: %s (%s)",
                movie_data.get("Title"),
                movie_data.get("Year"),
            )
            movie_results.append(movie_data)

        if not movie_results:
            # Fallar explícitamente: si no, el DAG terminaría en verde sin
            # haber hecho nada y sería difícil detectar el problema.
            raise ValueError(
                "No se obtuvo ninguna película. Revisa la API key y los títulos."
            )

        log.info("Se obtuvieron %d películas.", len(movie_results))
        return movie_results

    @task
    def transform_movie_info(raw_movies: list[dict]) -> list[dict]:
        """
        Transforma los datos crudos de la API en un formato limpio.

        Extrae los campos relevantes de cada película.
        Para cada película en raw_movies, crea un diccionario con:
        - "title": título (campo "Title" de la API)
        - "year": año (campo "Year")
        - "director": director (campo "Director")
        - "rating_imdb": puntuación IMDb (campo "imdbRating"), convertida a float
        - "genre": género (campo "Genre")
        - "plot": sinopsis corta (campo "Plot")

        """
        clean_movies: list[dict] = []

        for movie in raw_movies:
            # .get() con default evita KeyError si la API omite un campo;
            # _to_float() evita el ValueError cuando imdbRating es "N/A".
            clean_movie = {
                "title": movie.get("Title") or "Desconocido",
                "year": movie.get("Year") or "Desconocido",
                "director": movie.get("Director") or "Desconocido",
                "rating_imdb": _to_float(movie.get("imdbRating")),
                "genre": movie.get("Genre") or "Desconocido",
                "plot": movie.get("Plot") or "Sin sinopsis",
            }
            clean_movies.append(clean_movie)
            log.info("Transformada: %s", clean_movie["title"])

        log.info("Se transformaron %d películas.", len(clean_movies))
        return clean_movies

    @task
    def display_results(movies: list[dict]) -> None:
        """
        Muestra los resultados del análisis.

        Muestra:
        - Un listado de todas las películas con su rating
        - La película con mejor puntuación IMDb
        - El promedio de puntuación de todas las películas

        Usa log.info() en vez de print().
        """
        if not movies:
            log.warning("No hay películas para mostrar.")
            return

        # 1) Listado de todas las películas con su rating.
        for movie in movies:
            rating = movie.get("rating_imdb", 0.0)
            rating_text = f"{rating:.1f}" if rating > 0 else "sin puntuación"
            log.info(
                "%s (%s) - IMDb: %s",
                movie.get("title", "Desconocido"),
                movie.get("year", "Desconocido"),
                rating_text,
            )

        # 2) Mejor puntuación y promedio. Excluimos los 0.0 (películas sin
        # rating) para que no distorsionen el promedio.
        rated_movies = [m for m in movies if m.get("rating_imdb", 0.0) > 0]

        if not rated_movies:
            log.warning("Ninguna película tiene puntuación IMDb válida.")
            return

        best_movie = max(rated_movies, key=lambda m: m.get("rating_imdb", 0.0))
        log.info(
            "Mejor película: %s con %.1f",
            best_movie.get("title", "Desconocido"),
            best_movie.get("rating_imdb", 0.0),
        )

        average_rating = sum(m.get("rating_imdb", 0.0) for m in rated_movies) / len(
            rated_movies
        )
        log.info(
            "Promedio de puntuación (%d películas con rating): %.2f",
            len(rated_movies),
            average_rating,
        )

    # --- Dependencias ---
    # Con TaskFlow API (@task), las dependencias se crean automáticamente
    # al pasar el return de una tarea como parámetro de la siguiente.
    # Además, los datos se mueven entre tareas vía XCom sin código extra.
    #
    # Esto equivale a: fetch >> transform >> display
    # pero con paso de datos incluido. La sintaxis ">>" solo define orden
    # de ejecución sin mover datos (se usa con operadores tradicionales).
    raw_data = fetch_movie_data()
    clean_data = transform_movie_info(raw_data)
    display_results(clean_data)


exercise_movie_pipeline()
