Este es un pipeline basado en Netflix, en el que usamos Kafka, Spark y Hadoop para procesar dummy data emulando el comportamiento de la empresa.
** Kafka **
- 1 Producer
- 2 Consumers: batch, streaming

Tenemos 3 instancias EC2 conectadas con Kafka, dentro de un sistema distribuido.
La configuración consta de 1 topic con 2 particiones. Los consumidores forman parte del mismo grupo.

** Spark **
Usamos Apache Spark para el procesamiento de los datos de manera flexible.
Creamos un dataframe con la localización, género y usuario.
Realizamos 3 operaciones: conteo de la frecuencia de visualización y agrupación de género y localizaciones.

** Hadoop **
Las 3 instancias utilizan Hadoop como almacenamiento. 
Después de realizar el procesamiento  de los datos con Spark, se guardan 3 tablas en HDFS.
Nos conectamos a HDFS a través de WebHDFS y Streamlit. Estos frameworks nos permiten visualizar los datos de forma estructurada y compactada dentro de tablas y gráficos, dentro de una red pública.
