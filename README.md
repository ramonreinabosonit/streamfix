# Instalación

Para ejecutar este proyecto es necesario tener instalado la versión 3.3 de Spark y la 2.12 de Scala, además de las variables de entorno HADOOP_HOME y JAVA_HOME.

Posteriormente se ejecutará el siguiente comando para arrancar el jar con spark-submit:
`spark-submit \
--class com.streamflix.Main \
target/scala-2.12/streamflixanalytics_2.12-0.1.jar \
src/main/resources/data/movies_metadata.csv \
src/main/resources/data/server_logs.txt \
src/main/resources/output`

Las rutas deberán de ser cambiadas a las rutas del dispositivo en el que se va a ejecutar la aplicación.

# Proyecto Scala StreamFlix

Módulo 1 Pregunta Reflexiva (Anti-IA):

- ¿Por qué usarías `reduceByKey` en lugar de `groupByKey` si tuviéramos millones de líneas de logs?

Porque se encarga de reducir la información que se envía por lo que mejora el rendimiento y disminuye la memoria. 
Mientras que groupByKey envía todos los datos sin reducción aplicada, para este caso, enviaría millones de logs...

- ¿Qué ocurriría en memoria si usamos groupByKey con 200M de registros donde una clave concentra
el 80% de los datos?

Ocurriría un problema con el uso de memoria y recursos ya que estaríamos aplicando groupByKey sobre 160 millones de datos
aproximadamente por lo que provocará un consumo excesivo de recursos, lentitud o incluso, errores por la falta de memoria.

- Justifica tu respuesta con el concepto de "Shuffling".

Es un proceso en el que Spark redistribuye datos entre los nodos. Cuando usamos groupByKey realmente estamos
usando Shuffle, ya que agrupamos todos los valores a una clave, mientras que si lo comparamos con reduceByKey,
primero aplica el reduce y luego se hace el Shuffle, por ello se utilizan muchos menos recursos.

---------------

Módulo 3 Validación Manual:

-   El alumno debe explicar el "Physical Plan" (enrichedDF.explain()) y señalar dónde
aparece BroadcastHashJoin.

El Physical Plan es el plan de ejecución de Spark, básicamente el cómo va a ejecutar spark la consulta. Podemos verlo
como ejemplo en el Join ya que se referencian ambos DataFrames y podríamos interpretarlo como una vista más cercana
a la ejecución real. El BroadcastHashJoin aparece en la operación, indicando que Spark distribuye el DataFrame
más pequeño a todos sus ejecutores para así optimizar el join.

-   ¿Qué ocurriría si el 60% de los logs pertenecen a la misma película?

Funcionaría y en la mayoría de los casos se podría trabajar con estos datos, lo único que habría particiones desiguales.
Existiría esta partición que sería muy grande mientras otras serían muy pequeñas. Esto provocaría mucha carga de trabajo
sobre una única partición provocando un peor rendimiento.

-   Resultado esperado: Una tabla ordenada por horas totales de visualización.
    
=== TOP 5 GENEROS MAS VISTOS ===

    +-------+-----------+
    |  genre|total_hours|
    +-------+-----------+
    | Horror|  167128394|
    | Action|  165792889|
    |  Drama|   49537427|
    | Sci-Fi|   48422718|
    |Romance|   48091311|
    +-------+-----------+
    only showing top 5 rows

Módulo 4 Validación Manual:

-    El dataset contiene un usuario User:999 que tiene tiempos negativos (errores
     de reloj). El alumno debe identificarlo y explicar cómo su lógica manejó ese caso (¿lo
     filtró antes o rompió el cálculo?).

Para controlar este error, lo que he decidido hacer es descartar el ID 999, tras cargar los datos. Antes de ejecutar
cualquier operación creé un nuevo DataFrame en el que se le aplica la siguiente consulta:

`// para no romper el análisis, descartamos el id 999 que tiene valores disonantes
val cleanLogsDF = logsDF.filter(col("user_id") =!= 999)`

Creo que esta es la mejor solución, dado que el usuario 999 tiene miles de registros y modificarlos sin afectar al
análisis es muy complicado, por ello, es mejor descartarlo y no tenerlo en cuenta en el análisis.

-   ID de los 3 usuarios más adictos

En primera posición debería de aparecer el ID 999 con unos 16.000 maratones, sin embargo al haberlo descartado
no aparece.

     +-------+---------------+--------+
     |user_id|total_maratones|is_binge|
     +-------+---------------+--------+
     |   1699|              4|    true|
     |  28350|              4|    true|
     |   1689|              4|    true|
     +-------+---------------+--------+
     only showing top 3 rows

Módulo 5 Validación Manual:

-   El alumno debe mostrar una captura de pantalla de su explorador de archivos mostrando
    la estructura de carpetas creada:
    `/output/analytics_warehouse/year=2023/country=ES/part-0000.parquet
    /output/analytics_warehouse/year=2023/country=US/part-0000.parquet`

![Resultado](C:\Users\ramon.reina\IdeaProjects\streamflix\src\main\resources\screenshots\cap3.png)

-    ¿Por qué NO debemos particionar por user_id en este caso? (Pista: Small files problem)

Desde luego que no es conveniente hacer una partición por user_id, la explicación es que si particionamos por user_id
el resultado serán miles de directorios o incluso millones (según los registros que tengamos). Además en el interior
de ellas tendremos poca información, quizás algunas acciones del usuario. De ahí el ''Small files problem'' ya que
los arhcivos del interior de los directorios serían minúsculos.