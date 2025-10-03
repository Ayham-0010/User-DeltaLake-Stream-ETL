from .consts import *
from .logger import *

def read_kafka_topic(topic_name, spark, kafka_server):
    logger.info(f"reading from {topic_name} kafka topic")
    kafka_stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    return kafka_stream_df




def merge_to_delta(microBatchOutputDF, batchId, table_name, key): 
    microBatchOutputDF.createOrReplaceTempView("updates")
    merge_sql = f"""
    MERGE INTO {table_name} t
    USING updates s
    ON s.{key} = t.{key}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    microBatchOutputDF.sparkSession.sql(merge_sql)




def write_to_delta(df, merge_to_delta, path, table_name, key):
    logger.info(f"writing to {table_name} delta table")
    query = df.writeStream \
             .format("delta") \
             .foreachBatch(lambda batchDF, batchId: merge_to_delta(batchDF, batchId, table_name, key)) \
             .outputMode("update") \
             .option("checkpointLocation", base_path + path + "checkpoints/") \
             .start(base_path + path)
    return query
