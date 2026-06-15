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