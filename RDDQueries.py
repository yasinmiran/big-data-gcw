from pyspark import SparkContext, SparkConf
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, decode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from analytics.functions import get_browser_category_by_ua, get_os_from_ua, get_device_from_ua, get_browser_vendor
from analytics.udfs import toBrowserType, toOperatingSystem, toDevice, toBrowserVendor
from utils.common import parse_line, USER_AGENT, IP_ADDR, REMOTE_USER, TIME_LOCAL, HTTP_METHOD, RESOURCE_URL, \
    HTTP_VERSION, STATUS, BYTES_SENT, HTTP_REFERER


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
        .load("/Users/yasin/bda-cw-workdir/access-logs.data")

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

    # Which type of device?
    # What is the OS its running?
    # From what browser category it accessed? (normal, crawlers)
    # Different types of browser vendors?

    # t1 = table.select(
    #     toDevice(col('USER_AGENT')).getItem(0).alias("brand"),
    #     toDevice(col('USER_AGENT')).getItem(1).alias("model"),
    #     toOperatingSystem(col('USER_AGENT')).alias("os"),
    #     toBrowserType(col('USER_AGENT')).alias("accessed_by"),
    #     toBrowserVendor(col('USER_AGENT')).alias("vendor"),
    # )

    ips = table.select(col('IP_ADDR')).distinct().count()
    print("Count ", ips)
    # t1.show(10, truncate=False)


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
