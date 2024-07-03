from confluent_kafka import Consumer, KafkaException
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, sum as _sum

conf = {
    'bootstrap.servers': 'Slave2v2:9092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False  # Disable auto commit for batch processing
}

spark = SparkSession.builder \
    .appName("KafkaConsumerWithSparkBatch") \
    .getOrCreate()

schema = "user_location STRING, total_count LONG"
total_user_locations = spark.createDataFrame([], schema)

def consume_and_process_with_spark_batch():
    global total_user_locations
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
            current_location_counts = df.groupBy(
                'user_location').agg(count('*').alias('count'))

            current_location_counts = current_location_counts.withColumnRenamed(
                'count', 'total_count')

            total_user_locations = total_user_locations.union(current_location_counts) \
                .groupBy('user_location').agg(_sum(col('total_count')).alias('total_count'))

            total_user_locations.show()

            consumer.commit()
            total_user_locations.write.mode('overwrite').csv(
                "hdfs://Masterv2:9000/spark")

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
        spark.stop()


# Call the consumer function
if __name__ == '__main__':
    consume_and_process_with_spark_batch()
