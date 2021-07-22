from pyspark import SparkContext, SparkConf
from operator import add

from analytics.functions import test_browser
from utils.common import parse_line, USER_AGENT, TIME_LOCAL


def main():
    """
    Main execution point
    :return: void
    """

    conf = SparkConf().setAppName("RDDQueries").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("resources/access-logs.data")
    log_data = lines.map(parse_line)

    # Browser family as "key", count as "value"
    browser_share = log_data \
        .map(lambda log: (test_browser(log[USER_AGENT]), 1)) \
        .reduceByKey(add)

    # log_data \
    #     .map(lambda log: (datetime.strptime(log[TIME_LOCAL], '%Y-%m'), log[TIME_LOCAL])) \
    #     .reduceByKey(lambda tup: )
    # TODO: Graph countries visited with hist YY-MMM.
    # TODO: Get percentages of browsers and crawler bots. (Done)
    # TODO: Most popular operating system with device?.
    # TODO: Count all the different browsers.

    for browser in browser_share.collect():
        print(browser)


if __name__ == "__main__":
    main()
