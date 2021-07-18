"""
    Determine all the browsers accessed this web-sever.
    This script will list [ (browser, request_count), ... ]
"""

from pyspark import SparkContext, SparkConf
from operator import add
from user_agents import parse


def parse_line(line):
    fields = line.split("\t")
    # (IP Address, Bytes Sent, UserAgent String)
    user_agent = parse(str(fields[9]))
    return (str(fields[0]), int(fields[7]), str(user_agent.browser.family))


def main():
    conf = SparkConf().setAppName("FindBrowserPercentage")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("hdfs:///user/maria_dev/access-logs/clean.data")
    log_data = lines.map(parse_line)

    # Browser family as "key", count as "value"
    browser_share = log_data.map(lambda log: (log[2], 1)).reduceByKey(add)
    # TODO: Sort the results
    # TODO: Pick the top 10
    # TODO: Plug spark streaming

    for browser in browser_share.collect():
        print(browser)


if __name__ == "__main__":
    main()
