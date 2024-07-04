from confluent_kafka import Consumer, KafkaException
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, sum as _sum

conf = {
    'bootstrap.servers': 'Slave1v2:9092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
}

spark = SparkSession.builder \
    .appName("KafkaConsumerWithSpark") \
    .getOrCreate()

schema = "user_location STRING, genre STRING, title STRING, watchfrequency LONG"
total_data = spark.createDataFrame([], schema)

def consume_and_process_with_spark():
    global total_data
    consumer = Consumer(conf)

    try:
        consumer.subscribe(['Proyecto'])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            value = msg.value().decode('utf-8')
            netflix_data = json.loads(value)

            df = spark.createDataFrame([netflix_data])

            df = df.withColumn('watch_count', lit(1))
          
            current_data_counts = df.groupBy('user_location', 'genre', 'title') \
                                    .agg(_sum(col('watch_count')).alias('watchfrequency'))

            total_data = total_data.union(current_data_counts) \
                .groupBy('user_location', 'genre', 'title').agg(_sum(col('watchfrequency')).alias('watchfrequency'))

            total_data.show()

            user_location_counts = total_data.groupBy('user_location').agg(count('*').alias('user_count'))
            user_location_counts.write.mode('overwrite').csv("hdfs://Masterv2:9000/sparkv2/Slave1v2/Locations")
          
            genre_counts = total_data.groupBy('genre').agg(count('*').alias('genre_count'))
            genre_counts.write.mode('overwrite').csv("hdfs://Masterv2:9000/sparkv2/Slave1v2/Genres")

            total_data.write.mode('overwrite').csv("hdfs://Masterv2:9000/sparkv2/Slave1v2/Users")

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
        spark.stop()

if __name__ == '__main__':
    consume_and_process_with_spark()
