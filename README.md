# Predicción de la Premier League: Dixon-Coles + Spark

Proyecto académico que combina un modelo estadístico de predicción de resultados de
fútbol (**Dixon-Coles**, 1997) con **PySpark**, para comparar una implementación
secuencial frente a su paralelización. El objetivo principal no es tanto maximizar
la precisión predictiva como practicar paralelización con Spark sobre un problema
de simulación Monte Carlo real.

## Contenido del repositorio

| Fichero | Descripción |
|---|---|
| `prediccion_premier_league.ipynb` | Notebook principal: carga de parámetros, simulación secuencial de partidos/liga, backtesting de una jornada real, versión Spark y comparación de tiempos. |
| `estimacion_parametros.ipynb` | Ajusta por máxima verosimilitud los parámetros del modelo (fuerza de ataque/defensa por equipo, ventaja de local, `rho`) a partir de los datos históricos de [football-data.co.uk](https://www.football-data.co.uk/englandm.php), descargados directamente por URL. |
| `estimaciones_2324.json` | Parámetros entrenados con las temporadas 20/21 a 23/24 (usados para predecir la 24/25). |
| `estimaciones_2425.json` | Parámetros entrenados con las temporadas 20/21 a 24/25 (usados para predecir la 25/26). |
| `jornadas_2025_26.json` | Resultados reales de las jornadas 1 a 19 de la 25/26, usados como test en el backtesting. |
| `requirements.txt` | Dependencias de Python. |

`estimacion_parametros.ipynb` es opcional para reproducir el notebook principal: los
JSON de parámetros ya están generados y versionados, así que `prediccion_premier_league.ipynb`
puede ejecutarse directamente sin volver a ajustar el modelo.

## El modelo

El número de goles de cada equipo se modela como una Poisson cuya media depende de la
fuerza atacante del equipo, la fuerza defensiva del rival y una ventaja de jugar en casa.
Dixon-Coles añade sobre el Poisson básico:

- Una corrección `rho` para los marcadores bajos (0-0, 1-0, 0-1, 1-1), que el modelo
  Poisson simple infraestima.
- Una ponderación temporal exponencial, para que los partidos recientes pesen más que
  los antiguos en la estimación.

Los parámetros se ajustan por máxima verosimilitud (`scipy.optimize.minimize`,
L-BFGS-B), con la restricción de identificabilidad habitual sobre los factores de
ataque. El desarrollo matemático completo está en la sección 1 de
`prediccion_premier_league.ipynb`.

## Secuencial vs. Spark

El notebook implementa dos veces la simulación (secuencial y con Spark) para tres
funciones, cada una paralelizando un nivel distinto de la simulación Monte Carlo:

- `estimar_partido` / `estimar_partido_spark`: paraleliza las `MC` simulaciones de un
  mismo partido.
- `simular_liga` / `simular_liga_spark`: paraleliza los `N×(N-1)` enfrentamientos de
  una liga completa.
- `estimar_liga` / `estimar_liga_spark`: paraleliza las `B` simulaciones completas de
  liga.

`estimar_partido` (la versión secuencial) no es redundante aunque exista su par en
Spark: es la función que usa el resto del notebook (backtesting de apuestas, etc.) y
además sirve como referencia de tiempos frente a `estimar_partido_spark` en la
comparación final.

En las mediciones registradas en el notebook (Google Colab, entorno con pocos cores),
Spark solo aventaja ligeramente a la versión secuencial porque el overhead de
serializar, hacer *broadcast* de los parámetros y repartir tareas entre pocos cores
consume buena parte de la ganancia teórica. La sección 4 y la conclusión del notebook
comentan esto con los tiempos reales medidos.

## Cómo ejecutarlo en local

El notebook se desarrolló en Google Colab, que solo ofrece 2 vCPUs compartidas en el
tier gratuito — eso limita mucho lo que Spark puede paralelizar de verdad, y además
añade overhead de sesión/IO con Drive. **Ejecutar en local aprovecha mejor Spark**
siempre que el JDK sea el correcto, porque cualquier portátil/PC de gama media actual
ya tiene más cores utilizables por `local[*]` que el Colab gratuito.

1. **Instalar un JDK de 64 bits (17 o superior)**. PySpark 4.x requiere Java 17+, y
   necesita ser una JVM de 64 bits para poder reservar varios GB de heap (`spark.driver.memory`
   en el notebook pide 4g). Comprobar con:

   ```bash
   java -version
   ```

   Si el resultado indica una instalación de 32 bits (por ejemplo, bajo
   `Program Files (x86)` en Windows) o una versión anterior a 17, hay que instalar un
   JDK de 64 bits (p. ej. [Eclipse Temurin 17](https://adoptium.net/)) y apuntar
   `JAVA_HOME` a esa instalación antes de lanzar Spark.

2. **Crear un entorno virtual e instalar dependencias**:

   ```bash
   python -m venv .venv
   .venv\Scripts\activate      # Windows
   # source .venv/bin/activate  # Linux/Mac
   pip install -r requirements.txt
   ```

3. **Lanzar Jupyter y ejecutar el notebook**:

   ```bash
   jupyter notebook prediccion_premier_league.ipynb
   ```

   La sesión de Spark ya está configurada en el notebook con `master('local[*]')`, así
   que usará automáticamente todos los cores disponibles en la máquina.

4. Para regenerar los JSON de parámetros desde cero (opcional, tarda más porque
   descarga y reajusta el modelo), ejecutar antes `estimacion_parametros.ipynb`.

### Sobre las simulaciones grandes

La comparación de tiempos de la sección 4 (`B=200` ligas completas) puede tardar del
orden de una hora incluso en local, porque cada liga simula `N×(N-1)` partidos con
`MC` iteraciones Monte Carlo cada uno. Para explorar el notebook sin esperar tanto,
basta con reducir `MC` y `B` en esa celda antes de ejecutarla; los resultados ya
guardados en el notebook corresponden a una ejecución completa de referencia.

## Fuentes de datos

- Resultados históricos: [football-data.co.uk](https://www.football-data.co.uk/englandm.php)
- Paper original: Dixon, M.J. y Coles, S.G. (1997), *"Modelling Association Football
  Scores and Inefficiencies in the Football Betting Market"*
  ([PDF](https://www.ajbuckeconbikesail.net/wkpapers/Airports/MVPoisson/soccer_betting.pdf))
- Artículo de referencia seguido para la implementación: [dashee87.github.io](https://dashee87.github.io/football/python/predicting-football-results-with-statistical-modelling-dixon-coles-and-time-weighting/)

## Contexto

Proyecto hecho en grupo para practicar programación secuencial vs. paralela con
Apache Spark, aplicado a un problema de simulación Monte Carlo con un modelo
estadístico real de por medio.
