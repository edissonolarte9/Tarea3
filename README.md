# Tarea3
Analisis de datos fifa

Este proyecto implementa un sistema de análisis de datos en tiempo real utilizando Apache Kafka y Apache Spark. Se simulan datos de jugadores de fútbol que son enviados mediante Kafka y procesados con Spark Streaming para obtener información en tiempo real.

Requisitos
Python 3.10
Apache Kafka
Apache Spark
Librería kafka-python
Estructura del proyecto

proyecto_fifa/
│
├── fifa_producer.py
├── spark_streaming_consumer.py
├── fifa.csv

Ejecución del proyecto

1️Iniciar Zookeeper
cd kafka_2.12-3.9.2
bin/zookeeper-server-start.sh config/zookeeper.properties

Iniciar Kafka
bin/kafka-server-start.sh config/server.properties

Crear el topic
bin/kafka-topics.sh --create \
--bootstrap-server localhost:9092 \
--topic fifa-stream \
--partitions 1 \
--replication-factor 1

Ejecutar el producer
python3 fifa_producer.py

Ejecutar el streaming
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
spark_streaming_consumer.py

Resultados

El sistema muestra en consola el número de jugadores por país en tiempo real, actualizándose continuamente a medida que se reciben nuevos datos.


