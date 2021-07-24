from operator import add

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, decode, col, count, struct
from pyspark.sql.types import StructType, StructField, StringType

from analytics.functions import get_browser_category_by_ua, get_os_from_ua
from analytics.udfs import get_browser_category, \
    get_operating_system, \
    get_device_vendor, \
    get_browser_vendor, \
    get_client_device_type
from utils.common import parse_line, USER_AGENT


def main():
    """

    :return:
    """

    # Create the spark session.
    spark = SparkSession.builder \
        .master("local[6]") \
        .appName("sample") \
        .getOrCreate()

    # kafka_df is just to replicate prod data env.
    # /Users/yasin/bda-cw-workdir/
    kafka_df = spark \
        .read \
        .format("text") \
        .option("header", "false") \
        .load("resources/access-logs-10k.data")

    # Define the schema for our access logs structured data line.
    log_schema = StructType([
        StructField("IP_ADDR", StringType(), True),
        StructField("REMOTE_USER", StringType(), True),
        StructField("TIME_LOCAL", StringType(), True),
        StructField("HTTP_METHOD", StringType(), True),
        StructField("RESOURCE_URL", StringType(), True),
        StructField("HTTP_VERSION", StringType(), True),
        StructField("STATUS", StringType(), True),
        StructField("BYTES_SENT", StringType(), True),
        StructField("HTTP_REFERER", StringType(), True),
        StructField("USER_AGENT", StringType(), True),
    ])

    table = kafka_df \
        .select(split(decode(col("value"), "utf-8"), "\t")) \
        .rdd \
        .flatMap(lambda x: x) \
        .toDF(schema=log_schema)

    # We want below data: -
    #
    # Which type of device?
    # What is the OS its running?
    # From what browser category it accessed? (normal, crawlers)
    # Different types of browser vendors?

    features = table.select(
        get_client_device_type(col('USER_AGENT')).alias("device_type"),
        get_device_vendor(col('USER_AGENT')).getItem(0).alias("brand"),
        get_device_vendor(col('USER_AGENT')).getItem(1).alias("model"),
        get_operating_system(col('USER_AGENT')).alias("os"),
        get_browser_category(col('USER_AGENT')).alias("accessed_by"),  # Either Client or Crawler
        get_browser_vendor(col('USER_AGENT')).alias("browser_vendor"),
    )

    # Queries
    # ----------
    # Let's say our Big Data Solution's Client is XYZ

    # First, XYZ wants to know how many crawlers and clients
    # access the website.
    browser_categories = features \
        .groupby(col("accessed_by")) \
        .agg(count("accessed_by").alias("count")) \
        .select(struct(col("accessed_by"), col("count")).alias("browser_categories"))

    # Then XYZ want to know what types of browsers their "Clients"
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
        .select(struct(col("brand"), col("count")).alias("types_of_brands"))

    # XYZ wants to infer what type of devices they use.
    # (Say for example iPhone 11, iPhone 12)
    types_of_devices = features \
        .filter(col("accessed_by") == "Crawler") \
        .groupby(col("device_type")) \
        .agg(count("device_type").alias("count")) \
        .sort(col("count").desc()) \
        .select(struct(col("device_type"), col("count")).alias("types_of_devices"))

    types_of_vendors.show(truncate=False)
    types_of_operating_systems.show(truncate=False)
    types_of_brands.show(truncate=False)
    types_of_devices.show(truncate=False)
    browser_categories.show(truncate=False)
    # writer_query.awaitTermination()

    return  # BELOW is STREAMING STUFF! Use above show() functions to infer results.

    # Multi query sinks
    # -------
    # Prepare out processed data to Kafka sink

    target_sink = "access-logs-sink"

    browser_categories \
        .selectExpr("to_json(struct(*)) as value") \
        .writeStream \
        .queryName("StreamWriter") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", target_sink) \
        .outputMode("complete") \
        .option("checkpointLocation", "checkpoints-dir") \
        .start()

    types_of_vendors \
        .selectExpr("to_json(struct(*)) as value") \
        .writeStream \
        .queryName("StreamWriter") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", target_sink) \
        .outputMode("complete") \
        .option("checkpointLocation", "checkpoints-dir") \
        .start()

    types_of_operating_systems \
        .selectExpr("to_json(struct(*)) as value") \
        .writeStream \
        .queryName("StreamWriter") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", target_sink) \
        .outputMode("complete") \
        .option("checkpointLocation", "checkpoints-dir") \
        .start()

    types_of_brands \
        .selectExpr("to_json(struct(*)) as value") \
        .writeStream \
        .queryName("StreamWriter") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", target_sink) \
        .outputMode("complete") \
        .option("checkpointLocation", "checkpoints-dir") \
        .start()

    types_of_devices \
        .selectExpr("to_json(struct(*)) as value") \
        .writeStream \
        .queryName("StreamWriter") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "access-logs-processed") \
        .outputMode("complete") \
        .option("checkpointLocation", "checkpoints-dir") \
        .start()

    spark.streams.awaitAnyTermination()


# def reduce_dataframes(dfs):
#     return functools \
#         .reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)


def main2():
    """
    Main execution point
    :return: void
    """

    conf = SparkConf().setAppName("RDDQueries").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("resources/access-logs.data")
    log_data = lines.map(parse_line)

    # Ultimately what we want is a summary of our access logs data!
    #
    # From where it accessed? x
    # Which type of device?
    # What is the OS its running?
    # From what browser category it accessed? (normal, crawlers)
    # Different types of browser vendors?

    # TODO: How do we get geo location data?
    # bigdatacloud APIs?
    #
    # Need a efficient strategy.
    # First collect all the

    # Check device types.
    # brand_with_device = log_data \
    #     .map(lambda log: get_device_from_ua(log[USER_AGENT])) \
    #     .toDF("brand", "model")
    # brand_with_device.show()

    # grouped_by_brand = brand_with_device \
    #     .groupBy(lambda x: x[0])
    # brands_with_devices = grouped_by_brand \
    #     .map(lambda x: (x[0], brand_with_device.map(lambda y: y[1]).distinct()))

    # To check whats the
    os_family = log_data \
        .map(lambda log: (get_os_from_ua(log[USER_AGENT]), 1)) \
        .reduceByKey(add)

    # Browser cat as "key", count as "value"
    browser_cat = log_data \
        .map(lambda log: (get_browser_category_by_ua(log[USER_AGENT]), 1)) \
        .reduceByKey(add)

    browser_share = log_data \
        .map(lambda log: (get_browser_vendor(log[USER_AGENT]), 1)) \
        .reduceByKey(add)

    # log_data \
    #     .map(lambda log: (datetime.strptime(log[TIME_LOCAL], '%Y-%m'), log[TIME_LOCAL])) \
    #     .reduceByKey(lambda tup: )
    # TODO: Graph countries visited with hist YY-MMM.
    # TODO: Get percentages of browsers and crawler bots. (Done)
    # TODO: Most popular operating system with device?.
    # TODO: Count all the different browsers.

    # for device in brands_with_devices.collect():
    #     print(device)


if __name__ == "__main__":
    main()

# # First, XYZ wants to know how many crawlers and clients
#     # access the website.
#     browser_categories = features \
#         .groupby(col("accessed_by")) \
#         .agg(count("accessed_by").alias("count")) \
#         .select(struct(col("accessed_by"), col("count")))
#
#     # Then XYZ want to know what types of browsers their "Clients"
#     # use. Say for example, Chrome, Edge, ...
#     types_of_vendors = features \
#         .filter(col("accessed_by") == "Crawler") \
#         .groupby(col("browser_vendor")) \
#         .agg(count("browser_vendor").alias("count")) \
#         .sort(col("count").desc()) \
#         .select(struct(col("browser_vendor"), col("count"))) \
#         .localCheckpoint()
#
#     # XYZ wants to know what types of Operating Systems their real customers use.
#     # Not including bots or crawlers.
#     types_of_operating_systems = features \
#         .filter(col("accessed_by") == "Crawler") \
#         .groupby(col("os")) \
#         .agg(count("os").alias("count")) \
#         .sort(col("count").desc()) \
#         .select(struct(col("os"), col("count")))
#
#     # XYZ wants to know what is types of device brands their clients use
#     # to access our site.
#     types_of_brands = features \
#         .filter(col("accessed_by") == "Crawler") \
#         .groupby(col("brand")) \
#         .agg(count("brand").alias("count")) \
#         .sort(col("count").desc()) \
#         .select(struct(col("brand"), col("count")))
#
#     # XYZ wants to infer what type of devices they use.
#     # (Say for example iPhone 11, iPhone 12)
#     types_of_devices = features \
#         .filter(col("accessed_by") == "Crawler") \
#         .groupby(col("device_type")) \
#         .agg(count("device_type").alias("count")) \
#         .sort(col("count").desc()) \
#         .select(struct(col("device_type"), col("count")))
