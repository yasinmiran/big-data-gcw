from crawlerdetect import CrawlerDetect


def test_browser(user_agent_string):
    """
    Checks whether a incoming request is from a bot or a normal
    client browser. It returns either "NormalBrowsers" or "Crawlers"
    as a generalized category.

    :param user_agent_string: {str} User-Agent string.
    :return: ∈ of {NormalBrowsers, Crawlers}
    """
    crawler_detect = CrawlerDetect()
    if crawler_detect.isCrawler(user_agent_string):
        return "NormalBrowsers"
    else:
        return "Crawlers"
