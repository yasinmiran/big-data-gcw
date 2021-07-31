from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, decode, count, struct

from analytics.udfs import get_browser_category, get_device_vendor, \
    get_operating_system, get_client_device_type, get_browser_vendor
from utils import logging
from utils.common import IP_ADDR, \
    USER_AGENT, HTTP_REFERER, BYTES_SENT, \
    HTTP_VERSION, RESOURCE_URL, HTTP_METHOD, \
    TIME_LOCAL, REMOTE_USER, STATUS


def main():
    """
    If you inspect closely, you see we don't create a Spark Cluster locally
    or connect to a remote cluster. For this CW demonstration we will keep it simple
    to make sure we understand the core concept.
    :return: void
    """

    """
    master: `local[2]` defines how many cores will be used to parallelize the
    RDD/DF process. In our case will use 2 from the host machine. Optionally,
    `local[*]` to allocate all the cores. Also, check spark-default.conf to
    using configurations.
    """
    spark = SparkSession \
        .builder \
        .appName("Access Logs Analytics Application") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    # .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:configs/log4j.properties") \
    # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \

    # Setup the Logger for the application.
    logger = logging.Log4j(spark)

    """
    Subscribe to the topic that we created. It will send send data
    from multiple streams of data.
    """
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "access-logs") \
        .option("startingOffsets", "earliest") \
        .load()

    # kafka_df is just to replicate prod data env.
    # /Users/yasin/bda-cw-workdir/
    # kafka_df = spark \
    # .read \
    # .format("text") \
    # .option("header", "false") \
    # .load("resources/access-logs-10k.data")

    logger.info("Read stream initialized...")

    # This is the message schema sent from kafka
    kafka_df.printSchema()

    # ------------

    # Define the schema for our access logs structured data line.
    # log_schema = StructType([
    #     StructField("IP_ADDR", StringType(), True),
    #     StructField("REMOTE_USER", StringType(), True),
    #     StructField("TIME_LOCAL", StringType(), True),
    #     StructField("HTTP_METHOD", StringType(), True),
    #     StructField("RESOURCE_URL", StringType(), True),
    #     StructField("HTTP_VERSION", StringType(), True),
    #     StructField("STATUS", StringType(), True),
    #     StructField("BYTES_SENT", StringType(), True),
    #     StructField("HTTP_REFERER", StringType(), True),
    #     StructField("USER_AGENT", StringType(), True),
    # ])
    #
    # table = kafka_df \
    #     .select(split(decode(col("value"), "utf-8"), "\t")) \
    #     .rdd \
    #     .flatMap(lambda x: x) \
    #     .toDF(schema=log_schema)

    # ------------

    # Split the data by tab separator.
    split_columns = kafka_df.select(split(decode(col("value"), "utf-8"), "\t").alias("line"))

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

    # Print the transformed schema.
    table.printSchema()

    # Select the required columns.
    features = table.select(
        get_client_device_type('USER_AGENT').alias("device_type"),
        get_device_vendor('USER_AGENT').getItem(0).alias("brand"),
        get_device_vendor('USER_AGENT').getItem(1).alias("model"),
        get_operating_system('USER_AGENT').alias("os"),
        get_browser_category('USER_AGENT').alias("accessed_by"),  # Either Client or Crawler
        get_browser_vendor('USER_AGENT').alias("browser_vendor"),
    )

    # Queries
    # ----------
    # Let's say our Big Data Solution's Client is XYZ

    # First, XYZ wants to know how many crawlers and clients
    # access the website.
    browser_categories = features \
        .groupby(col("accessed_by")) \
        .agg(count(col("accessed_by")).alias("count")) \
        .select(struct(col("accessed_by"), col("count")).alias("browser_categories"))

    # Then XYZ wants to know what types of browsers their "Clients"
    # use. Say for example, Chrome, Edge, ...
    types_of_vendors = features \
        .filter(col("accessed_by") == "Crawler") \
        .groupby(col("browser_vendor")) \
        .agg(count("browser_vendor").alias("count")) \
        .sort(col("count").desc()) \
        .select(struct(col("browser_vendor"), col("count")).alias("types_of_vendors"))

    # XYZ wants to know what types of Operating Systems their real customers use.
    # Not including bots or crawlers.
    types_of_operating_systems = features \
        .filter(col("accessed_by") == "Crawler") \
        .groupby(col("os")) \
        .agg(count("os").alias("count")) \
        .sort(col("count").desc()) \
        .select(struct(col("os"), col("count")).alias("types_of_operating_systems"))

    # XYZ wants to know what is types of device brands their clients use
    # to access our site.
    types_of_brands = features \
        .filter(col("accessed_by") == "Crawler") \
        .groupby(col("brand")) \
        .agg(count("brand").alias("count")) \
        .sort(col("count").desc()) \
        .filter(col("brand").isNotNull()) \
        .select(struct(col("brand"), col("count")).alias("types_of_brands"))

    # XYZ wants to infer what type of devices they use.
    # (Say for example Pc, Tablet, Mobile)
    types_of_devices = features \
        .filter(col("accessed_by") == "Crawler") \
        .groupby(col("device_type")) \
        .agg(count("device_type").alias("count")) \
        .sort(col("count").desc()) \
        .select(struct(col("device_type"), col("count")).alias("types_of_devices"))

    # types_of_vendors.show(truncate=False)
    # types_of_operating_systems.show(truncate=False)
    # types_of_brands.show(truncate=False)
    # types_of_devices.show(truncate=False)
    # browser_categories.show(truncate=False)
    # writer_query.awaitTermination()

    # Multi query sinks
    # -------
    # Prepare out processed data to Kafka sink

    target_sink = "access-logs-sink"

    browser_categories.selectExpr("to_json(struct(*)) as value") \
        .writeStream \
        .queryName("StreamWriter1") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", target_sink) \
        .outputMode("complete") \
        .option("checkpointLocation", "checkpoints/checkpoints-dir-01") \
        .start()

    types_of_vendors.selectExpr("to_json(struct(*)) as value") \
        .writeStream \
        .queryName("StreamWriter2") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", target_sink) \
        .outputMode("complete") \
        .option("checkpointLocation", "checkpoints/checkpoints-dir-02") \
        .start()

    types_of_operating_systems.selectExpr("to_json(struct(*)) as value") \
        .writeStream \
        .queryName("StreamWriter3") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", target_sink) \
        .outputMode("complete") \
        .option("checkpointLocation", "checkpoints/checkpoints-dir-03") \
        .start()

    types_of_brands.selectExpr("to_json(struct(*)) as value") \
        .writeStream \
        .queryName("StreamWriter4") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", target_sink) \
        .outputMode("complete") \
        .option("checkpointLocation", "checkpoints/checkpoints-dir-04") \
        .start()

    types_of_devices.selectExpr("to_json(struct(*)) as value") \
        .writeStream \
        .queryName("StreamWriter5") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", target_sink) \
        .outputMode("complete") \
        .option("checkpointLocation", "checkpoints/checkpoints-dir-05") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
