import os
import sys
import time

from pyspark.sql import SparkSession, Row
from pyspark import SparkContext
import pyspark


from core.logger import logger
from core.stream_pipeline import *





def main():
    logger.info("service started successfully")


    try:

        kafka_server = os.environ['KAFKA_BOOTSTRAP_SERVERS']

        logger.info("creating spark session")
        spark = (
                SparkSession.builder
                .appName("crud_to_deltalake_app")
                .config("spark.streaming.stopGracefullyOnShutdown", True)
                .getOrCreate()
            )

        logger.info("spark session created")
        
        spark.sql(f"set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;")
        logger.info("CDC enabled in delta tables")

        for path, table_name in zip(paths, table_names):
            try:
                spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{base_path + path}'")
                logger.info(f"{table_name} table created at'{base_path + path}'")
            except:
                continue

        logger.info("Streaming from crud system to delta lake strated")
        stream_kafka_to_deltalake(spark ,kafka_server)



    except Exception as e:
         logger.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
