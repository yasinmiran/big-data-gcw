import user_agents
from crawlerdetect import CrawlerDetect


def get_device_from_ua(user_agent_string):
    data = user_agents.parse(user_agent_string)
    return [data.device.brand, data.device.model]


def get_os_from_ua(user_agent_string):
    data = user_agents.parse(user_agent_string)
    return _unknown_if_empty(data.os.family)


def get_browser_category_by_ua(user_agent_string):
    """
    Checks whether a incoming request is from a bot or a normal
    client browser. It returns either "NormalBrowsers" or "Crawlers"
    as a generalized category.

    :param user_agent_string: {str} User-Agent string.
    :return: ∈ of {Client, Crawlers}
    """
    crawler_detect = CrawlerDetect()
    if crawler_detect.isCrawler(user_agent_string):
        return "Client"
    else:
        return "Crawler"


def get_browser_vendor_name(user_agent_string):
    data = user_agents.parse(user_agent_string)
    return _unknown_if_empty(data.browser.family)


def get_client_type(user_agent_string):
    data = user_agents.parse(user_agent_string)
    if data.is_pc:
        return "Pc"
    elif data.is_mobile:
        return "Mobile"
    elif data.is_tablet:
        return "Tablet"
    elif data.is_bot:
        return "Bot or Crawler"
    elif data.is_email_client:
        return "Email Client"
    else:
        return "Unspecified"


def _unknown_if_empty(test):
    if len(test) > 0:
        return test
    else:
        return "Unknown"
