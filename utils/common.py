# Input data split indexes.
IP_ADDR = 0
REMOTE_USER = 1
TIME_LOCAL = 2
HTTP_METHOD = 3
RESOURCE_URL = 4
HTTP_VERSION = 5
STATUS = 6
BYTES_SENT = 7
HTTP_REFERER = 8
USER_AGENT = 9


def parse_line(line):
    """
    Parses the raw log string. Return a tuple with ordered
    indexes. Use above indexes to access them.
    :param line: {str} access log
    :return: {tuple}
    """
    fields = line.split("\t")
    return (str(fields[IP_ADDR]),
            str(fields[REMOTE_USER]),
            str(fields[TIME_LOCAL]),
            str(fields[HTTP_METHOD]),
            str(fields[RESOURCE_URL]),
            str(fields[HTTP_VERSION]),
            int(fields[STATUS]),
            int(fields[BYTES_SENT]),
            str(fields[HTTP_REFERER]),
            str(fields[USER_AGENT]))
