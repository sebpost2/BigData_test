from confluent_kafka import Consumer, KafkaException
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as _sum

# Kafka configuration
conf = {
    'bootstrap.servers': 'Slave1v2:9092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False  # Disable auto commit for batch processing
}

spark = SparkSession.builder \
    .appName("KafkaConsumerWithSparkBatch") \
    .getOrCreate()

schema = "user_location STRING, genre STRING, title STRING, watchfrequency LONG"
total_data = spark.createDataFrame([], schema)

def consume_and_process_with_spark_batch():
    global total_data
    consumer = Consumer(conf)

    try:
        consumer.subscribe(['Proyecto'])  # Subscribe to Kafka topic 'Proyecto'

        while True:
            messages = consumer.consume(num_messages=10, timeout=10.0)

            if not messages:
                continue

            batch_data = []
            for msg in messages:
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break

                value = msg.value().decode('utf-8')
                netflix_data = json.loads(value)
                batch_data.append(netflix_data)

            df = spark.createDataFrame(batch_data)

            df = df.withColumn('watch_count', lit(1))

            current_data_counts = df.groupBy('user_location', 'genre', 'title') \
                                    .agg(_sum(col('watch_count')).alias('watchfrequency'))

            total_data = total_data.union(current_data_counts)

            total_data.show()

            consumer.commit()

    except KeyboardInterrupt:
        pass

    except Exception as e:
        print("Error in consumer loop:", e)

    finally:
        # Write results to CSV files
        write_location_counts(total_data)
        write_genre_counts(total_data)
        write_user_genre_location(total_data)

        consumer.close()
        spark.stop()

def write_location_counts(df):
    try:
        location_counts = df.groupBy('user_location').agg(_sum(col('watchfrequency')).alias('user_count'))

        location_counts.write.mode('overwrite').csv("hdfs://Masterv2:9000/sparkv2/Slave1v2/Locations")
        print("Saved location_counts.csv successfully.")
    except Exception as e:
        print("Error saving location_counts.csv:", e)

def write_genre_counts(df):
    try:
        genre_counts = df.groupBy('genre').agg(_sum(col('watchfrequency')).alias('genre_count'))

        user_genre_location.write.mode('overwrite').csv("hdfs://Masterv2:9000/sparkv2/Slave1v2/Users")
        print("Saved user_genre_location.csv successfully.")
    except Exception as e:
        print("Error saving user_genre_location.csv:", e)
        
def write_user_genre_location(df):
    try:
        user_genre_location = df.select('user_location', 'genre', 'title', 'watchfrequency')

        user_genre_location.write.mode('overwrite').csv("hdfs://Masterv2:9000/sparkv2/Slave1v2/Users")
        print("Saved user_genre_location.csv successfully.")
    except Exception as e:
        print("Error saving user_genre_location.csv:", e)

if __name__ == '__main__':
    consume_and_process_with_spark_batch()

