from confluent_kafka import Consumer, KafkaException
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, sum as _sum

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
}

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaConsumerWithSpark") \
    .getOrCreate()

# Initialize an empty DataFrame to accumulate user locations
schema = "user_location STRING, total_count LONG"
total_user_locations = spark.createDataFrame([], schema)

# Kafka consumer function
def consume_and_process_with_spark():
    global total_user_locations
    consumer = Consumer(conf)

    try:
        consumer.subscribe(['Proyecto'])  # Subscribe to Kafka topic 'Proyecto'

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

            # Process received message
            value = msg.value().decode('utf-8')
            netflix_data = json.loads(value)

            # Create DataFrame from the received JSON data
            df = spark.createDataFrame([netflix_data])

            # Count user locations in the current message
            current_location_counts = df.groupBy('user_location').agg(count('*').alias('count'))

            # Rename the count column to match the total_user_locations schema
            current_location_counts = current_location_counts.withColumnRenamed('count', 'total_count')

            # Merge current counts with total_user_locations DataFrame
            total_user_locations = total_user_locations.union(current_location_counts)\
                .groupBy('user_location').agg(_sum(col('total_count')).alias('total_count'))

            # Show total counts in the console
            total_user_locations.show()

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
        spark.stop()

# Call the consumer function
if __name__ == '__main__':
    consume_and_process_with_spark()