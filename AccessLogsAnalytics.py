import codecs
from operator import add

from crawlerdetect import CrawlerDetect
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, decode, udf, count
from pyspark.sql.types import StringType

from analytics.udfs import toBrowserType
from utils import logging
from utils.common import IP_ADDR, REMOTE_USER, TIME_LOCAL, HTTP_METHOD, RESOURCE_URL, HTTP_VERSION, STATUS, BYTES_SENT, \
    HTTP_REFERER, USER_AGENT


def main():
    """
    If you inspect closely, you see we don't create a Spark Cluster locally
    or connect to a remote cluster. For this CW demonstration we will keep it simple
    to make sure we understand the core concept.
    :return: void
    """

    """
    <Create Spark Session>
        
    master: `local[2]` defines how many cores will be used to parallelize the
    RDD/DF process. In our case will use 2 from the host machine. Optionally,
    `local[*]` to allocate all the cores.
    
    
    spark.driver.extraJavaOptions: Enables the logging with our custom Log4J
    properties file.
    
    spark.jars.packages: includes the spark sql streaming library driver for
    kafka.
    
    """
    spark = SparkSession \
        .builder \
        .appName("Access Logs Analytics Application") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    #        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:configs/log4j.properties") \
    #         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \

    # Setup the Logger for the application.
    logger = logging.Log4j(spark)

    """
    <Load Kafka Stream>
    
    Subscribe to the topic that we created. It will send send data
    from multiple streams of data.
    
    """
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "access-logs") \
        .option("startingOffsets", "earliest") \
        .load()

    logger.info("Read stream initialized...")

    # This is the message schema sent from kafka
    kafka_df.printSchema()

    # Split the data by tab separator.
    split_columns = kafka_df.select(
        split(
            decode(col("value"), "utf-8"),
            "\t"  # H-TAB
        ).alias("line")
    )

    # Structure the data properly. Basically, after we splitting the
    # string now we need a proper structured data frame to operate
    # on. Alias is the column name. You can check the current with
    # `df.show()`
    table = split_columns.select(
        col("line").getItem(IP_ADDR).cast("string").alias("IP_ADDR"),
        col("line").getItem(REMOTE_USER).cast("string").alias("REMOTE_USER"),
        col("line").getItem(TIME_LOCAL).cast("string").alias("TIME_LOCAL"),
        col("line").getItem(HTTP_METHOD).cast("string").alias("HTTP_METHOD"),
        col("line").getItem(RESOURCE_URL).cast("string").alias("RESOURCE_URL"),
        col("line").getItem(HTTP_VERSION).cast("string").alias("HTTP_VERSION"),
        col("line").getItem(STATUS).cast("int").alias("STATUS"),
        col("line").getItem(BYTES_SENT).cast("int").alias("BYTES_SENT"),
        col("line").getItem(HTTP_REFERER).cast("string").alias("HTTP_REFERER"),
        col("line").getItem(USER_AGENT).cast("string").alias("USER_AGENT"))

    # Determine browser category percentage.
    browser_types = table \
        .withColumn('USER_AGENT', toBrowserType(col('USER_AGENT'))) \
        .groupby(col("USER_AGENT").alias("ua")).count()

    # query = kafka_df.writeStream \
    #     .format("text") \
    #     .queryName("Sample Query") \
    #     .outputMode("append") \
    #     .option("path", "output") \
    #     .option("checkpointLocation", "checkpoints") \
    #     .trigger(processingTime="5 seconds") \
    #     .start()

    query = browser_types \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    logger.warn("Listening for Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
